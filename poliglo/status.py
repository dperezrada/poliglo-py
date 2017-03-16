# -*- coding: utf-8 -*-
from time import time

import poliglo.variables as var
from poliglo.utils import to_json

def update_done_jobs(connection, workflow, instance_id, worker_id, job_id, start_time):
    connection.sadd(
        var.REDIS_KEY_INSTANCE_WORKER_JOBS % (workflow, instance_id, worker_id, 'done'),
        job_id
    )
    connection.lpush(
        var.REDIS_KEY_INSTANCE_WORKER_TIMING % (workflow, instance_id, worker_id),
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
            var.REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id),
            key,
            value
        )
    pipe.execute()

def get_workflow_instance_key(connection, workflow, workflow_instance_id, key):
    return connection.hget(
        var.REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id),
        key
    )

def update_workflow_instance_key(connection, workflow, workflow_instance_id, key, value):
    connection.hset(
        var.REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id),
        key,
        value
    )

def workflow_instance_exists(connection, workflow, workflow_instance_id):
    return connection.exists(var.REDIS_KEY_ONE_INSTANCE % (workflow, workflow_instance_id))

def stats_add_new_instance(connection, workflow, workflow_instance_info):
    connection.zadd(var.REDIS_KEY_INSTANCES % workflow, time(), to_json(workflow_instance_info))

def get_queue_message(connection, meta_worker, timeout=0):
    return connection.brpoplpush(var.REDIS_KEY_QUEUE % meta_worker, var.REDIS_KEY_QUEUE_PROCESSING % meta_worker, timeout)

def undo_mark_meta_worker_as_processed(connection, meta_worker, timeout=0):
    return connection.brpoplpush(var.REDIS_KEY_QUEUE_PROCESSING % meta_worker, var.REDIS_KEY_QUEUE % meta_worker, timeout)

def move_meta_worker_to_worker_id_queue(connection, meta_worker, worker_id):
    return connection.brpoplpush(var.REDIS_KEY_QUEUE_PROCESSING % meta_worker, var.REDIS_KEY_QUEUE_PROCESSING % worker_id)

def mark_worker_id_as_finalized(connection, meta_worker, raw_data):
    return connection.lrem(var.REDIS_KEY_QUEUE_PROCESSING % meta_worker, -1, raw_data)
