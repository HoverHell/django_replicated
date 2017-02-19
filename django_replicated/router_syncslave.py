# coding: utf-8

from __future__ import unicode_literals

from .router import ReplicationRouterBase, ReplicationRouter


class SyncSlaveRouterMixin(object):
    """ An addition to ReplicationRouter that supports synchronous slaves """

    REPLICATED_DATABASE_SYNC_SLAVES = []

    def __init__(self):
        super(SyncSlaveRouterMixin, self).__init__()
        self.all_allowed_aliases = self.all_allowed_aliases + self._get_setting('database_sync_slaves')
        raise Exception("TODO")


class ReplicationRouterSyncSlave(SyncSlaveRouterMixin, ReplicationRouter):
    pass
