# -*- coding: utf-8 -*-
from uuid import uuid4
from copy import deepcopy
from time import time
import traceback

from poliglo.variables import REDIS_KEY_INSTANCE_WORKER_JOBS, REDIS_KEY_QUEUE, \
    REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS, REDIS_KEY_QUEUE_FINALIZED, \
    REDIS_KEY_INSTANCE_WORKER_ERRORS
from poliglo.utils import to_json, json_loads, convert_object_to_unicode
from poliglo.status import update_workflow_instance

def add_data_to_next_worker(connection, output, raw_data):
    connection.lpush(REDIS_KEY_QUEUE % output, raw_data)

def add_new_job_id(connection, workflow, instance_id, worker, job_id):
    connection.sadd(
        REDIS_KEY_INSTANCE_WORKER_JOBS % (
            workflow, instance_id, worker, 'total'
        ), job_id
    )

def write_one_output(connection, output_meta_worker, output_worker_id, workflow_instance_data):
    new_job_id = str(uuid4())
    workflow_instance_data['jobs_ids'] = workflow_instance_data['jobs_ids'] + [new_job_id]
    workflow_instance_data['workflow_instance']['worker_id'] = output_worker_id
    workflow_instance_data['workflow_instance']['meta_worker'] = output_meta_worker
    add_new_job_id(
        connection,
        workflow_instance_data['workflow_instance']['workflow'],
        workflow_instance_data['workflow_instance']['id'],
        output_worker_id,
        new_job_id
    )

    add_data_to_next_worker(connection, output_meta_worker, to_json(workflow_instance_data))

def prepare_write_output(workflow_instance_data, worker_output_data, worker_id):
    new_workflow_instance_data = deepcopy(workflow_instance_data)
    if not new_workflow_instance_data['workflow_instance'].get('workers'):
        new_workflow_instance_data['workflow_instance']['workers'] = []
    new_workflow_instance_data['workflow_instance']['workers'].append(worker_id)
    if not new_workflow_instance_data.get('workers_output'):
        new_workflow_instance_data['workers_output'] = {}
    new_workflow_instance_data['workers_output'][worker_id] = worker_output_data
    new_workflow_instance_data['inputs'] = worker_output_data
    return new_workflow_instance_data

def write_outputs(connection, workflow_instance_data, worker_output_data, worker_workflow_data):
    new_workflow_instance_data = prepare_write_output(
        workflow_instance_data,
        worker_output_data,
        workflow_instance_data['workflow_instance']['worker_id']
    )
    update_workflow_instance(
        connection,
        new_workflow_instance_data['workflow_instance']['workflow'],
        new_workflow_instance_data['workflow_instance']['id']
    )
    # TODO: use pipline
    # pipe = connection.pipeline()
    workers_outputs_types = worker_workflow_data.get('__next_workers_types', [])
    for i, output_worker_id in enumerate(worker_workflow_data.get('next_workers', [])):
        write_one_output(
            connection, workers_outputs_types[i], output_worker_id, new_workflow_instance_data
        )
    # pipe.execute()

def write_finalized_job(workflow_instance_data, worker_output_data, worker_id, connection):
    # prepare_write_output(workflow_instance_data, worker_output_data, worker_id):
    new_workflow_instance_data = prepare_write_output(
        workflow_instance_data, worker_output_data, worker_id
    )
    connection.zadd(
        REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS % (
            new_workflow_instance_data['workflow_instance']['workflow'],
            new_workflow_instance_data['workflow_instance']['id'], worker_id
        ),
        time(),
        to_json(new_workflow_instance_data)
    )
    connection.lpush(
        REDIS_KEY_QUEUE_FINALIZED,
        to_json({
            'workflow': new_workflow_instance_data['workflow_instance']['workflow'],
            'workflow_instance_id': new_workflow_instance_data['workflow_instance']['id'],
            'workflow_instance_name': new_workflow_instance_data['workflow_instance'].get(
                'name', 'untitled'),
            # TODO: be able to always return a valid meta_worker
            'meta_worker': new_workflow_instance_data['workflow_instance'].get(
                'meta_worker', 'not sure'
            ),
            'worker_id': worker_id
        })
    )

def write_error_job(connection, worker_id, raw_data, error):
    metric_name = 'errors'
    try:
        workflow_instance_data = json_loads(raw_data)
        if not workflow_instance_data.get('workers_error'):
            workflow_instance_data['workers_error'] = {}
        workflow_instance_data['workers_error'][worker_id] = {
            'error': str(error), 'traceback': traceback.format_exc()
        }
        metric_name = REDIS_KEY_INSTANCE_WORKER_ERRORS % (
            workflow_instance_data['workflow_instance']['workflow'],
            workflow_instance_data['workflow_instance']['id'],
            worker_id
        )
    except Exception, e:
        workflow_instance_data = {'workers_error': {}, 'raw_data': raw_data}
        workflow_instance_data['workers_error'][worker_id] = {
            'error': 'cannot json_loads', 'traceback': traceback.format_exc()
        }
        metric_name = REDIS_KEY_INSTANCE_WORKER_ERRORS % (
            'unknown', 'unknown', worker_id
        )
    try:
        json_encoded = to_json(workflow_instance_data)
    except Exception, e:
        json_encoded = to_json(convert_object_to_unicode(workflow_instance_data))
    connection.zadd(metric_name, time(), json_encoded)
