# coding: utf8

# ### Global settings, e.g. the ones used in the middleware. ###

# Cache backend name to store database state
REPLICATED_CACHE_BACKEND = None

# Cookie name for read-after-write workaround
REPLICATED_FORCE_MASTER_COOKIE_NAME = 'just_updated'

# View name to state mapping
REPLICATED_VIEWS_OVERRIDES = {}

# Header name for forcing state switch
REPLICATED_FORCE_STATE_HEADER = 'HTTP_X_REPLICATED_STATE'

# Status codes on which set cookie for read-after-write workaround
REPLICATED_FORCE_MASTER_COOKIE_STATUS_CODES = (302, 303)

# Cookie life time in minutes
REPLICATED_FORCE_MASTER_COOKIE_MAX_AGE = 5

# ### Settings used in both router and middleware ###

# Timeout for dead databases alive check for read only flag
REPLICATED_READ_ONLY_DOWNTIME = 20

# Number of retries before set read only flag
REPLICATED_READ_ONLY_TRIES = 1
