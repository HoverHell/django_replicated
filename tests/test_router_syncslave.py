# coding: utf-8

from __future__ import unicode_literals

import pytest
import mock

from django import db
from django.db import models

from django_replicated.router_multidb import OverridesReplicationRouter
from .test_router import model

pytestmark = pytest.mark.django_db


@pytest.fixture
def syncslave_settings(settings):
    base_config = settings.DATABASES['default']
    settings.DATABASES.update(
        sync_slave_1=dict(base_config),
        sync_slave_2=dict(base_config),
    )
    settings.DATABASE_ROUTERS = ['django_replicated.router_syncslave.ReplicationRouterSyncSlave']
    settings.REPLICATED_DATABASE_SYNC_SLAVES = ['sync_slave_1', 'sync_slave_2']
    settings.REPLICATED_CHECK_STATE_ON_WRITE = False


@pytest.yield_fixture
def django_router(syncslave_settings):
    # Init with the new settings.
    res = db.ConnectionRouter()
    # Mock needed for the `django_replicated.utils.routers` code.
    with mock.patch.object(db, 'router', new=res):
        yield res


def test_router_syncslave(django_router, model):
    from django_replicated.utils import routers
    routers.init('master_forced')  # Same as in `ReplicationMiddleware`

    router = django_router
    assert router.db_for_read(model).startswith('sync_slave_'), "should read from a sync slave here"
    assert router.db_for_write(model) == 'default'
    assert router.db_for_read(model) == 'default', "read after writing should go to master"
