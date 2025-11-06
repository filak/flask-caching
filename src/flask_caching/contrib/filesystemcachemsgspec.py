# -*- coding: utf-8 -*-
"""
    flask_caching.backends.filesystem_msgspec
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    FileSystemCache backend using msgspec for serialization
    (optionally with zlib compression).

    Based on FileSystemCacheGzJson (custom MOD).
"""
import errno
import hashlib
import logging
import os
import stat
import sys
import tempfile
import zlib
from time import time

import msgspec
from flask_caching.backends.base import BaseCache

logger = logging.getLogger(__name__)


class FileSystemCacheMsgspec(BaseCache):
    """Filesystem cache backend using msgspec for serialization.

    Supports optional zlib compression, just like your FileSystemCacheGzJson.
    """

    _fs_transaction_suffix = ".__wz_cache"
    _fs_count_file = "__wz_cache_count"

    def __init__(
        self,
        cache_dir,
        threshold=500,
        default_timeout=300,
        mode=0o600,
        hash_method=hashlib.md5,
        ignore_errors=False,
        compress=False,
        compress_level=3,
    ):
        super(FileSystemCacheMsgspec, self).__init__(default_timeout)
        self._path = cache_dir
        self._threshold = threshold
        self._mode = mode
        self._hash_method = hash_method
        self.ignore_errors = ignore_errors
        self.compress = compress
        self.compress_level = compress_level

        try:
            os.makedirs(self._path)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise

        if self._threshold != 0:
            self._update_count(value=len(self._list_dir()))

    @classmethod
    def factory(cls, app, config, args, kwargs):
        args.insert(0, config["CACHE_DIR"])
        kwargs.update(
            dict(
                threshold=config["CACHE_THRESHOLD"],
                ignore_errors=config["CACHE_IGNORE_ERRORS"],
                compress=config.get("CACHE_COMPRESS", False),
                compress_level=config.get("CACHE_COMPRESS_LEVEL", 3),
            )
        )
        return cls(*args, **kwargs)

    # ---------------------------
    # Internal bookkeeping
    # ---------------------------

    @property
    def _file_count(self):
        return self.get(self._fs_count_file) or 0

    def _update_count(self, delta=None, value=None):
        if self._threshold == 0:
            return
        if delta:
            new_count = self._file_count + delta
        else:
            new_count = value or 0
        self.set(self._fs_count_file, new_count, mgmt_element=True)

    def _normalize_timeout(self, timeout):
        timeout = BaseCache._normalize_timeout(self, timeout)
        if timeout != 0:
            timeout = time() + timeout
        return int(timeout)

    def _get_filename(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        hash = self._hash_method(key).hexdigest()
        return os.path.join(self._path, hash)

    def _list_dir(self):
        mgmt_files = [
            self._get_filename(name).split("/")[-1]
            for name in (self._fs_count_file,)
        ]
        return [
            os.path.join(self._path, fn)
            for fn in os.listdir(self._path)
            if not fn.endswith(self._fs_transaction_suffix)
            and fn not in mgmt_files
        ]

    # ---------------------------
    # Serialization helpers
    # ---------------------------

    def _serialize(self, obj):
        data = msgspec.msgpack.encode(obj)
        if self.compress:
            data = zlib.compress(data, level=self.compress_level)
        return data

    def _deserialize(self, data):
        if data is None:
            return None
        if self.compress:
            data = zlib.decompress(data)
        return msgspec.msgpack.decode(data)

    # ---------------------------
    # Core cache operations
    # ---------------------------

    def _prune(self):
        if self._threshold == 0 or not self._file_count > self._threshold:
            return
        entries = self._list_dir()
        now = time()
        nremoved = 0
        for idx, fname in enumerate(entries):
            try:
                with open(fname, "rb") as f:
                    expires, _ = self._deserialize(f.read())
                remove = (expires != 0 and expires <= now) or idx % 3 == 0
                if remove:
                    os.remove(fname)
                    nremoved += 1
            except Exception:
                pass
        self._update_count(value=len(self._list_dir()))
        logger.debug("evicted %d key(s)", nremoved)

    def clear(self):
        for fname in self._list_dir():
            try:
                os.remove(fname)
            except (IOError, OSError):
                self._update_count(value=len(self._list_dir()))
                return False
        self._update_count(value=0)
        return True

    def get(self, key):
        filename = self._get_filename(key)
        try:
            with open(filename, "rb") as f:
                expires, value = self._deserialize(f.read())
            if expires != 0 and expires < time():
                self.delete(key)
                return None
            return value
        except FileNotFoundError:
            return None
        except Exception as exc:
            logger.error("get key %r -> %s", key, exc)
            return None

    def set(self, key, value, timeout=None, mgmt_element=False):
        if mgmt_element:
            timeout = 0
        else:
            self._prune()

        timeout = self._normalize_timeout(timeout)
        filename = self._get_filename(key)
        tmpname = filename + ".__tmp"
        data = self._serialize([timeout, value])

        try:
            with open(tmpname, "wb") as f:
                f.write(data)
            is_new_file = not os.path.exists(filename)
            if not is_new_file:
                os.remove(filename)
            os.replace(tmpname, filename)
            if sys.platform == "win32":
                os.chmod(filename, stat.S_IWRITE)
            else:
                os.chmod(filename, self._mode)
        except Exception as exc:
            logger.error("set key %r -> %s", key, exc)
            return False
        else:
            if not mgmt_element and is_new_file:
                self._update_count(delta=1)
            return True

    def add(self, key, value, timeout=None):
        filename = self._get_filename(key)
        if not os.path.exists(filename):
            return self.set(key, value, timeout)
        return False

    def delete(self, key, mgmt_element=False):
        try:
            os.remove(self._get_filename(key))
            if not mgmt_element:
                self._update_count(delta=-1)
            return True
        except FileNotFoundError:
            return False
        except Exception as exc:
            logger.error("delete key %r -> %s", key, exc)
            return False

    def has(self, key):
        filename = self._get_filename(key)
        try:
            with open(filename, "rb") as f:
                expires, _ = self._deserialize(f.read())
            if expires != 0 and expires < time():
                self.delete(key)
                return False
            return True
        except FileNotFoundError:
            return False
        except Exception as exc:
            logger.error("has key %r -> %s", key, exc)
            return False
