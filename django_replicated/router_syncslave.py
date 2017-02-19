# coding: utf-8

from __future__ import unicode_literals

from .utils import shuffled
from .router import ReplicationRouterBase, ReplicationRouter


class SyncSlaveRouterMixin(object):
    """
    An addition to ReplicationRouter that supports synchronous slaves.

    It directs the reads to synchronous slaves rather than the master when the
    state is forced by the cookie.
    """

    REPLICATED_DATABASE_SYNC_SLAVES = []

    def __init__(self):
        super(SyncSlaveRouterMixin, self).__init__()
        self.all_allowed_aliases = self.all_allowed_aliases + self._get_setting('database_sync_slaves')

    def db_for_read(self, model, **hints):
        state = self.state()
        if state != 'master_forced':
            return super(SyncSlaveRouterMixin, self).db_for_read(model, **hints)

        # Caching
        try:
            return self.context.chosen[state]
        except KeyError:
            pass

        chosen = self._get_actual_sync_slave(model, **hints)
        self.context.chosen[state] = chosen
        return chosen

    def _get_actual_sync_slave(self, model, **kwargs):
        possibilities = shuffled(self._get_setting('database_sync_slaves'))
        return self._get_alive_database(possibilities, fallback=self.DEFAULT_DB)



class ReplicationRouterSyncSlave(SyncSlaveRouterMixin, ReplicationRouter):
    pass
