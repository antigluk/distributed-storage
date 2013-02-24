#!/bin/bash

# DATA_DIR="~root/"
# RUN_DIR="/var/run/"
# REPO_DIR="~root/app-repo/../"
# INTERNAL_IP="127.0.0.1"

# ================= Redis ====================

cd ${DATA_DIR}redis
chmod +x ${DATA_DIR}redis/bin/redis-server
[ -f ${RUN_DIR}redis.pid ] || ${DATA_DIR}redis/bin/redis-server ${DATA_DIR}redis/bin/redis.conf


# # ================= NAME SERVER ====================

# cd ${REPO_DIR}
# ${DATA_DIR}bin/gunicorn -w 4 -b $INTERNAL_IP:15001 app.ns.name:app --pid=/tmp/gunicorn-ns.pid \
#     --daemon --access-logfile=${DATA_DIR}gunicorn_access.log \
#     --error-logfile=${DATA_DIR}gunicorn_error.log >> ${DATA_DIR}gunicorn.log


# ================= Front-end ====================

chmod +x ${REPO_DIR}app/application

nohup ${DATA_DIR}bin/python ${REPO_DIR}app/application --logging=DEBUG >> ${DATA_DIR}/tornado.log 2>&1 &


# ================= Celery worker ====================

cd ${REPO_DIR}app
for x in `seq 1 3`; do
    nohup ${DATA_DIR}bin/celery worker -A api >> ${DATA_DIR}/celery_ns_worker$x.log 2>&1 &
done
