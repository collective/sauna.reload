# -*- coding: utf-8 -*-
# Copyright (c) 2011 University of Jyväskylä and Contributors.
#
# All Rights Reserved.
#
# Authors:
#     Esa-Matti Suuronen <esa-matti@suuronen.org>
#     Asko Soukka <asko.soukka@iki.fi>
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS

"""Database Adapters"""

from persistent.TimeStamp import TimeStamp

from zope.interface import implementer
from zope.component import adapter

from ZODB.interfaces import IDatabase
from ZODB.FileStorage.FileStorage import FileStorage, read_index
from ZEO.ClientStorage import ClientStorage
from ZEO.cache import ClientCache
from ZEO.asyncio.client import ClientThread

from sauna.reload.interfaces import IDatabaseHooks

import ZEO.asyncio.client


@implementer(IDatabaseHooks)
@adapter(IDatabase)
class ZODBDatabaseHooksAdapter(object):
    """
    Selective ZODB-adapter (e.g. supporting both FileStorage and ZEO)
    """

    def __init__(self, context):
        self.context = IDatabaseHooks(context.storage)

    def prepareForReload(self):
        return self.context.prepareForReload()

    def resumeFromReload(self):
        return self.context.resumeFromReload()


@implementer(IDatabaseHooks)
@adapter(FileStorage)
class ZODBFileStorageDatabaseHooksAdapter(object):
    """
    FileStorage-adapter
    """

    def __init__(self, context):
        # Try to get the *real* FileStorage,
        # because `context` may be just a BlobStorage-wrapper
        # and it wraps FileStorage differently between
        # ZODB3-3.9.5 and 3.10.x-series (eg. between Plone 4.0 and 4.1).
        self.context = getattr(context, '_BlobStorage__storage', context)

    def prepareForReload(self):
        # Save ``Data.fs.index`` before dying
        # to notify the next child
        # about all the presistent changes
        self.context._lock_acquire()
        try:
            self.context._save_index()
        finally:
            self.context._lock_release()

    def resumeFromReload(self):
        # Reload ``Data.fs.index``
        # to get up to date
        # about all the persistent changes
        self.context._lock_acquire()
        try:
            # Get indexes in their pre-fork state
            index, tindex = (self.context._index, self.context._tindex)
            # Load saved ``Data.fs.index`` to see the persistent changes
            # created by the previous child.
            index, start, ltid =\
                self.context._restore_index() or (None, None, None)
            # Sanity check. Last transaction in restored index must match
            # the last transaction given by FileStorage transaction iterator.
            if ltid and ltid == tuple(self.context.iterator())[-1].tid:
                self.context._initIndex(index, tindex)
                self.context._pos, self.context._oid, tid = read_index(
                    self.context._file, self.context._file_name, index, tindex,
                    stop='\377' * 8, ltid=ltid, start=start, read_only=False)
                self.context._ltid = tid
                self.context._ts = TimeStamp(tid)
        finally:
            self.context._lock_release()


@implementer(IDatabaseHooks)
@adapter(ClientStorage)
class ZEOClientStorageDatabaseHooksAdapter(object):
    """
    ZEO-client adapter

    Plone 5.1 support contribued by csanahuja
    https://github.com/collective/sauna.reload/issues/19
    """

    def __init__(self, context):
        self.context = context

        # Store cache settings
        self._cache_path = self.context._cache.path
        self._cache_size = self.context._cache.maxsize

        # Close the main process' connection (before forkloop)
        _server = self.context._server
        _server.close()

        # Close the main process' cache (before forkloop)
        self.context._cache.close()

    def prepareForReload(self):
        _server = self.context._server
        if self.context.is_connected():
            # Close the connection (before the child is killed)
            _server.close()

            # Close the cache (before the child is killed)
            self.context._cache.close()

    def resumeFromReload(self):
        # Open a new cache for the new child
        self.context._cache = ClientCache(
            self._cache_path, size=self._cache_size)

        # Prepare a new connection for the new child
        self.context._server = ClientThread(
            self.context._addr,
            self.context,
            self.context._cache,
            self.context._storage,
            (ZEO.asyncio.client.Fallback
             if self.context._read_only_fallback
             else self.context._is_read_only),
            None,
            ssl=None,
            ssl_server_hostname=None,
            credentials=None
        )

        self.context._call = self.context._server.call
        self.context._async = self.context._server.async_
        self.context._async_iter = self.context._server.async_iter
        self.context._wait = self.context._server.wait

        # Connect the new child to ZEO
        wait = True
        if wait:
            try:
                self.context._wait()
            except Exception:
                # No point in keeping the server going of the storage
                # creation fails
                self.context._server.close()
                raise
