# coding: utf-8

from __future__ import unicode_literals

import functools
import random
import copy

from django import db


def get_object_name(obj):
    try:
        return obj.__name__
    except AttributeError:
        return obj.__class__.__name__


def import_string(dotted_path):
    try:
        from django.utils.module_loading import import_string
    except ImportError:
        pass
    else:
        return import_string(dotted_path)
    # Support django1.6 too.
    # Copypaste of django1.6's django.db.utils.ConnectionRouter.__init__
    # But without most of error handling.
    from django.utils.importlib import import_module
    module_name, klass_name = dotted_path.rsplit('.', 1)
    module = import_module(module_name)
    return getattr(module, klass_name)


class Routers(object):
    """ Helper to affect all relevant routers. Used in middleware. """

    def __getattr__(self, name):
        methods = list(getattr(router, name, None) for router in db.router.routers)
        methods = list(method for method in methods if method is not None)
        if not methods:
            raise AttributeError("No router with the method %r." % (name,))
        return self._make_methods_wrapper(methods)

    @classmethod
    def _make_methods_wrapper(cls, methods):
        """
        A fake method that calls all relevant routers and returns *something*.

        Similar to django's ConnectionRouter._router_func, but calls all
        methods, and works as a helper to __getattr__.
        """
        assert methods, "requires a non-empty list"

        @functools.wraps(methods[0])
        def wrapped(*args, **kwargs):
            result = None
            for method in methods:
                method_result = method(*args, **kwargs)
                if result is None:
                    result = method_result
            return result

        return wrapped


routers = Routers()


class SettingsProxy(object):
    """ Wrapper for django settings with fallback to default """

    def __init__(self):
        from django.conf import settings as django_settings
        from . import settings as default_settings

        self.django_settings = django_settings
        self.default_settings = default_settings

    def __getattr__(self, key):
        try:
            return getattr(self.django_settings, key)
        except AttributeError:
            return getattr(self.default_settings, key)


def shuffled(value, **kwargs):
    """ What `random.shuffled` should have been """
    value = copy.copy(value)
    random.shuffle(value, **kwargs)
    return value
