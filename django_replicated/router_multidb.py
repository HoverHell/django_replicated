# coding: utf-8

from __future__ import unicode_literals

from .router import ReplicationRouterBase


class OverridesRouterMixin(object):
    """
    A mixin for ReplicationRouterBase that only processed models marked for its
    primary database.
    """

    REPLICATED_PRIMARY_DATABASE = None  # required to be overridden

    _model_override_attr = '_route_database'

    def _get_model_override(self, model):
        return getattr(model, self._model_override_attr, None)

    @property
    def DEFAULT_DB(self):
        return self._get_setting('primary_database')

    def _is_relevant_model(self, model, **hints):
        return self._get_model_override(model) == self.DEFAULT_DB

    def db_for_write(self, model, **hints):
        if not self._is_relevant_model(model, **hints):
            return None  # Not for this router
        return super(OverridesRouterMixin, self).db_for_write(model, **hints)

    def db_for_read(self, model, **hints):
        if not self._is_relevant_model(model, **hints):
            return None  # Not for this router
        return super(OverridesRouterMixin, self).db_for_read(model, **hints)

    def allow_relation(self, obj1, obj2, **hints):
        sup = super(OverridesRouterMixin, self).allow_relation(obj1, obj2, **hints)
        return sup or None  # either okay or ignore.



class OverridesReplicationRouter(OverridesRouterMixin, ReplicationRouterBase):
    """
    Usage:

      * Make a sublass (in a module importable without django settings).
      * Configure it anyhow
        * e.g. `self._update_from_dict(settings.REPLICATED_OTHER_SETTINGS)`
        * set 'REPLICATED_PRIMARY_DATABASE' to the primary of the database that
          will be handled by this router.
      * Add the resulting class at the beginning of the django routers.
    """
