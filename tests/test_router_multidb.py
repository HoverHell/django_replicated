# coding: utf-8

from __future__ import unicode_literals

import pytest
import mock

from django import db
from django.db import models

from django_replicated.router_multidb import OverridesReplicationRouter
from .test_router import model

pytestmark = pytest.mark.django_db


class DB2ReplicationRouter(OverridesReplicationRouter):
    def __init__(self):
        from django.conf import settings
        self._update_from_dict(settings.DB2_ROUTER_SETTINGS)
        super(DB2ReplicationRouter, self).__init__()


@pytest.fixture
def multidb_settings(settings):
    base_config = settings.DATABASES['default']

    # Many slaves to increase chances that the stochastic behavior is tested.
    slaves = ["db2_slave{}".format(idx) for idx in range(20)]
    extra_databases = dict(
        db2=dict(
            base_config,
        ),
    )
    extra_databases.update(
        (name, dict(base_config))
        for name in slaves)
    settings.DATABASES.update(extra_databases)  # XX: does the pytest-django's mockup handle this?

    settings.DB2_ROUTER_SETTINGS = dict(
        # Side note: mistakes in this are pretty hard to debug.
        primary_database='db2',
        database_slaves=slaves,
    )
    settings.DATABASE_ROUTERS = ['tests.test_router_multidb.DB2ReplicationRouter'] + settings.DATABASE_ROUTERS


@pytest.yield_fixture
def django_router(multidb_settings):
    # Init with the new settings.
    res = db.ConnectionRouter()
    # Mock needed for the `django_replicated.utils.routers` code.
    with mock.patch.object(db, 'router', new=res):
        yield res


@pytest.fixture
def mdb_model():

    class _TestModelMdb(models.Model):
        _route_database = 'db2'
        class Meta:
            app_label = 'django_replicated'

    return _TestModelMdb


@pytest.fixture
def mdb_model_2():

    class _TestModelMdb2(models.Model):
        _route_database = 'db2'
        class Meta:
            app_label = 'django_replicated'

    return _TestModelMdb2


def test_router_db_for_write(django_router, mdb_model):
    assert django_router.db_for_write(mdb_model) == 'db2'


def test_router_db_for_read(django_router, mdb_model, model):
    # Same code as middleware:
    from django_replicated.utils import routers
    routers.init('slave')

    router = django_router

    # `test_router_db_for_read`
    res1 = router.db_for_read(mdb_model)
    assert res1.startswith('db2_slave'), "supposed to select an overridden slave"
    res2 = router.db_for_read(mdb_model)
    assert res1 == res2, "supposed to select the same slave again"
    # Use the same router on another model to ensure its state does not screw up:
    # `test_router.test_router_db_for_read(router, model)`
    assert router.db_for_read(model) in ('slave1', 'slave2')


def test_router_allow_relation(django_router, model, mdb_model, mdb_model_2):
    from django_replicated.utils import routers

    models = [model, mdb_model, mdb_model_2]
    objs = [mdl() for mdl in models]

    for state in ('slave', 'master'):

        routers.init('slave')

        for obj in objs:
            obj._state.db = django_router.db_for_read(obj.__class__)

        obj1, obj2, obj3 = objs
        assert django_router.allow_relation(obj2, obj3)
        assert not django_router.allow_relation(obj1, obj2)
        assert not django_router.allow_relation(obj1, obj3)
