#!/bin/sh
if [ "x$STATSD_PREFIX" != x -a "x$STATSD_HOST" != x ]; then
    EXTRAS="--statsd-host $STATSD_HOST --statsd-prefix $STATSD_PREFIX"
fi
# for compatibility, if anyone runs this interactively:
if [ -x bin/start-pgbouncer ]; then
    START_PGBOUNCER=bin/start-pgbouncer
fi
if [ "x$PORT" = x ]; then
    PORT=8000
fi
$START_PGBOUNCER gunicorn --timeout 500 --pythonpath mcweb --bind 0.0.0.0:$PORT $EXTRAS mcweb.wsgi
