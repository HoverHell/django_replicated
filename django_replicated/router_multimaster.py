# coding: utf-8

from __future__ import unicode_literals

from .utils import shuffled
from .router import ReplicationRouter


class MultiMasterMixin(object):
    """ ... """

    # Whether the master db should be checked before use.
    REPLICATED_ALLOW_MASTER_FALLBACK = True

    REPLICATED_DATABASE_SUBMASTERS = []

    def _get_actual_master(self, model, **kwargs):
        chosen = getattr(self.context, 'actual_master', None)
        if chosen:
            if self._get_setting('allow_master_fallback'):
                if not self.is_alive(chosen):
                    chosen = None
        if chosen:
            return chosen
        # Be predictable here. No shuffle for master.
        default_db = self.DEFAULT_DB
        submasters = self._get_setting('database_submasters')
        chosen = self._get_alive_database([default_db] + submasters, fallback=default_db)
        self.context.actual_master = chosen
        return chosen

    def _get_possible_slaves(self, **kwargs):
        # Try masters too if slaves cannot be used,
        # Note: shuffled independently.
        slaves = shuffled(self._get_setting('database_slaves'))
        submasters = shuffled(self._get_setting('database_submasters'))
        return slaves + submasters


class ReplicationMultiMasterRouter(MultiMasterMixin, ReplicationRouter):
    """ ... """
