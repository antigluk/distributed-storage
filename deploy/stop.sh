#!/bin/bash

kill `ps -ef | grep application | grep -v grep | awk '{ print $2 }'` > /dev/null 2>&1
# kill `ps -ef | grep gunicorn | grep -v grep | awk '{ print $2 }'` > /dev/null 2>&1
kill `ps -ef | grep redis-server | grep -v grep | awk '{ print $2 }'` > /dev/null 2>&1
kill `ps -ef | grep "celery worker" | grep -v grep | awk '{ print $2 }'` > /dev/null 2>&1
exit 0