# coding: utf-8
from __future__ import unicode_literals

import random
from itertools import chain
from threading import local
from .utils import import_string
from django.utils.six import string_types

from .utils import SettingsProxy


class ReplicationRouter(object):

    def __init__(self):
        from django.db import DEFAULT_DB_ALIAS
        settings = SettingsProxy()

        self._context = local()

        self.DATABASES = settings.DATABASES
        self.DEFAULT_DB_ALIAS = DEFAULT_DB_ALIAS
        self.DOWNTIME = settings.REPLICATED_DATABASE_DOWNTIME
        self.SLAVES_LEGACY = settings.REPLICATED_DATABASE_SLAVES
        self.CHECK_STATE_ON_WRITE = settings.REPLICATED_CHECK_STATE_ON_WRITE

        wrapped_router_cls = settings.REPLICATED_WRAPPED_ROUTER
        if isinstance(wrapped_router_cls, string_types):
            wrapped_router_cls = import_string(wrapped_router_cls)
        self.wrapped_router = wrapped_router_cls()

        db_to_slaves = {}
        db_to_sync_slaves = {}
        db_to_other_masters = {}
        replica_kinds = (
            # (settings_key, mapping)
            ('SLAVE_TO', db_to_slaves),
            ('SUBMASTER_TO', db_to_other_masters),
            ('SYNC_SLAVE_TO', db_to_sync_slaves),
        )
        for db_name, db_conf in self.DATABASES.items():
            for conf_key, target_mapping in replica_kinds:
                conf_value = db_conf.get(conf_key)  # e.g. the name of the master database
                if conf_value:
                    # e.g. associate the current db as slave to that master database.
                    target_mapping.setdefault(conf_value, []).append(db_name)

        self.db_to_slaves = db_to_slaves
        self.db_to_sync_slaves = db_to_sync_slaves
        self.db_to_other_masters = db_to_other_masters

        # Legacy support: separate option with a list of slaves.
        # Append + uniq to the existing list.
        if self.SLAVES_LEGACY:
            def_slaves = db_to_slaves.get(self.DEFAULT_DB_ALIAS, [])
            def_slaves.extend(self.SLAVES_LEGACY)
            def_slaves = list(set(def_slaves))
            db_to_slaves[self.DEFAULT_DB_ALIAS] = def_slaves

        # Mapping from the database to its primary master, for `allow_relation`.
        db_to_master = {}
        all_mappings = chain(
            db_to_slaves.items(), db_to_sync_slaves.items(),
            db_to_other_masters.items())
        for db_name, db_slaves in all_mappings:
            for db_slave in db_slaves:
                db_to_master[db_slave] = db_name

        self.db_to_master = db_to_master

    def _init_context(self):
        self._context.state_stack = []
        self._context.chosen = {}
        self._context.state_change_enabled = True
        self._context.inited = True

    @property
    def context(self):
        if not getattr(self._context, 'inited', False):
            self._init_context()
        return self._context

    def init(self, state):
        self._init_context()
        self.use_state(state)

    def is_alive(self, db_name):
        from .dbchecker import db_is_alive

        return db_is_alive(db_name, self.DOWNTIME)

    def set_state_change(self, enabled):
        self.context.state_change_enabled = enabled

    def state(self):
        '''
        Current state of routing: 'master' or 'slave'.
        '''
        if self.context.state_stack:
            return self.context.state_stack[-1]
        else:
            return 'master'

    def use_state(self, state):
        '''
        Switches router into a new state. Requires a paired call
        to 'revert' for reverting to previous state.
        '''
        if not self.context.state_change_enabled:
            state = self.state()
        self.context.state_stack.append(state)
        return self

    def revert(self):
        '''
        Reverts wrapper state to a previous value after calling
        'use_state'.
        '''
        self.context.state_stack.pop()

    def _make_db_key(self, db, state=None):
        state = self.state() if state is None else state
        return "{}__{}".format(state, db)

    def db_for_write(self, model, **hints):
        if self.state() != 'master':
            if self.CHECK_STATE_ON_WRITE:
                raise RuntimeError('Trying to access master database in slave state')
            # (?) else: self.use_state('master'); but would need a revert too.
            # (?) else: self.context.state_stack[-1] = 'master'  # self.override_state('master')

        db_name = self.wrapped_router.db_for_write(model, **hints)
        assert db_name in self.DATABASES, \
            "wrapped router's db_for_write should return a known database"
        key = self._make_db_key(db_name)
        self.context.chosen[key] = db_name
        return db_name

    def db_for_read(self, model, **hints):
        if self.state() == 'master':
            return self.db_for_write(model, **hints)

        db_name = self.wrapped_router.db_for_read(model, **hints)
        assert db_name in self.DATABASES, \
            "wrapped router's db_for_read should return a known database"
        key = self._make_db_key(db_name)

        # Caching
        try:
            return self.context.chosen[key]
        except KeyError:
            pass

        slaves = self.db_to_slaves.get(db_name) or [db_name]
        slaves = slaves[:]  # copy
        random.shuffle(slaves)
        for slave in slaves:
            if self.is_alive(slave):
                chosen = slave
                break
        else:
            chosen = db_name

        self.context.chosen[key] = chosen

        return chosen

    def allow_relation(self, obj1, obj2, **hints):
        objs = [obj1, obj2]
        dbs = [self.db_to_master.get(obj._state.db) for obj in objs]
        db1, db2 = dbs
        return db1 == db2
