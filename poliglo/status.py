# -*- coding: utf-8 -*-
from time import time

from poliglo.variables import REDIS_KEY_INSTANCE_WORKER_JOBS, REDIS_KEY_INSTANCE_WORKER_TIMING, \
    REDIS_KEY_ONE_INSTANCE, REDIS_KEY_INSTANCES
from poliglo.utils import to_json

def update_done_jobs(connection, workflow, instance_id, worker_id, job_id, start_time):
    connection.sadd(
        REDIS_KEY_INSTANCE_WORKER_JOBS % (workflow, instance_id, worker_id, 'done'),
        job_id
    )
    connection.lpush(
        REDIS_KEY_INSTANCE_WORKER_TIMING % (workflow, instance_id, worker_id),
        time() - start_time
    )

def update_workflow_instance(
        connection, workflow, workflow_instance_id, workflow_instance_data=None
    ):
    if workflow_instance_data is None:
        workflow_instance_data = {}
    pipe = connection.pipeline()
    workflow_instance_data['update_time'] = time()
    for key, value in workflow_instance_data.iteritems():
        pipe.hset(
            REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id),
            key,
            value
        )
    pipe.execute()

def get_workflow_instance_key(connection, workflow, workflow_instance_id, key):
    return connection.hget(
        REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id),
        key
    )

def update_workflow_instance_key(connection, workflow, workflow_instance_id, key, value):
    connection.hset(
        REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id),
        key,
        value
    )

def workflow_instance_exists(connection, workflow, workflow_instance_id):
    return connection.exists(REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id))

def stats_add_new_instance(connection, workflow, workflow_instance_info):
    connection.zadd(REDIS_KEY_INSTANCES % workflow, time(), to_json(workflow_instance_info))

