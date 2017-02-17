# coding: utf-8

from __future__ import unicode_literals

import pytest
import mock

from django import db
from django.db import models

from .test_router import model
from django_replicated.router import ReplicationRouter

pytestmark = pytest.mark.django_db


@pytest.fixture
def multimaster_settings(settings):
    base_config = settings.DATABASES['default']
    settings.DATABASES.update(master2=dict(base_config, SUBMASTER_TO='default'))
    settings.REPLICATED_ALLOW_MASTER_FALLBACK = True


def test_router_multimaster(model, multimaster_settings):
    router = ReplicationRouter()

    assert router.db_for_write(model) == 'default'
    assert router.db_for_write(model) == 'default', 'Master should not be random on choices'
    assert router.db_for_write(model) == 'default', 'Master should not be random on choices'

    with mock.patch.object(router, 'is_alive') as is_alive_mock:

        def default_is_down(dbname):
            return False if dbname == 'default' else True

        def master2_is_down(dbname):
            return False if dbname == 'master2' else True

        def everything_is_up(dbname):
            return True

        is_alive_mock.side_effect = default_is_down
        assert router.db_for_write(model) == 'master2', 'Should switch to first working master on fail'

        is_alive_mock.side_effect = everything_is_up
        assert router.db_for_write(model) == 'master2', 'Chosen master should be kept unless failed'
        assert router.db_for_write(model) == 'master2', 'Chosen master should be kept unless failed'
        assert router.db_for_write(model) == 'master2', 'Chosen master should be kept unless failed'

        is_alive_mock.side_effect = master2_is_down
        assert router.db_for_write(model) == 'default', 'Should switch to first working master on fail'
