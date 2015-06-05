# -*- coding: utf-8 -*-
import uuid
import md5
import os
import traceback
import time
from copy import deepcopy

import redis

from poliglo.utils import select_dict_el, make_request, to_json, json_loads, convert_object_to_unicode

REDIS_KEY_QUEUE = 'queue:%s'
REDIS_KEY_QUEUE_FINALIZED = 'queue:finalized'
REDIS_KEY_INSTANCES = 'scripts:%s:processes'
REDIS_KEY_ONE_INSTANCE = "scripts:%s:processes:%s"
REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS = "scripts:%s:processes:%s:workers:%s:finalized"
REDIS_KEY_INSTANCE_WORKER_JOBS = "scripts:%s:processes:%s:workers:%s:jobs_ids:%s"
REDIS_KEY_INSTANCE_WORKER_ERRORS = "scripts:%s:processes:%s:workers:%s:errors"
REDIS_KEY_INSTANCE_WORKER_DISCARDED = "scripts:%s:processes:%s:workers:%s:discarded"

POLIGLO_SERVER_URL_WORKER_CONFIG = "%s/worker_types/%s/config"
POLIGLO_SERVER_URL_WORKER_SCRIPTS = "%s/worker_types/%s/scripts"

# Start Preparation methods
def get_connection(worker_config, target='redis'):
    if target == 'redis':
        return redis.StrictRedis(
            host=worker_config.get('REDIS_HOST'),
            port=worker_config.get('REDIS_PORT'),
            db=worker_config.get('REDIS_DB')
        )

def get_config(master_mind_url, worker_type):
    _, _, content = make_request(POLIGLO_SERVER_URL_WORKER_CONFIG % (master_mind_url, worker_type))
    worker_config = json_loads(content)
    return worker_config

def get_worker_script_data(worker_scripts, data, worker_id):
    worker_script_data = worker_scripts.get(select_dict_el(data, 'process.type'), {}).get(worker_id, {})

    if worker_script_data is None:
        worker_script_data = {}
    # TODO: check if deepcopy this is only needed for testing purpose
    return deepcopy(worker_script_data)

def prepare_worker(master_mind_url, worker_type):
    _, _, content = make_request(POLIGLO_SERVER_URL_WORKER_SCRIPTS % (master_mind_url, worker_type))
    worker_scripts = json_loads(content)
    worker_config = get_config(master_mind_url, worker_type)

    connection = get_connection(worker_config)
    return worker_scripts, connection

def get_inputs(data, worker_script_data):
    inputs = worker_script_data.get('default_inputs', {})
    select_inputs = select_dict_el(worker_script_data, 'before.select_inputs', {})
    for input_key, selector in select_inputs.iteritems():
        inputs[input_key] = select_dict_el(data, selector)
    inputs.update(data.get('inputs', {}))
    return inputs

def get_job_data(raw_data, encoding='utf-8'):
    data_to_loads = raw_data
    if not isinstance(raw_data, unicode):
        data_to_loads = raw_data.decode(encoding)
    return json_loads(data_to_loads)

# End Preparation methods

# Start Status and Stats methods

def add_data_to_next_worker(connection, output, raw_data):
    connection.lpush(REDIS_KEY_QUEUE % output, raw_data)

def update_done_jobs(connection, process_type, instance_id, worker_id, job_id):
    connection.sadd(
        REDIS_KEY_INSTANCE_WORKER_JOBS % (process_type, instance_id, worker_id, 'done'),
        job_id
    )

def add_new_job_id(connection, process_type, instance_id, worker, job_id):
    connection.sadd(
        REDIS_KEY_INSTANCE_WORKER_JOBS % (
            process_type, instance_id, worker, 'total'
        ), job_id
    )

def update_process(connection, process_type, process_id, data=None):
    if data is None:
        data = {}
    pipe = connection.pipeline()
    data['update_time'] = time.time()
    for key, value in data.iteritems():
        pipe.hset(
            REDIS_KEY_ONE_INSTANCE % (process_type, process_id),
            key,
            value
        )
    pipe.execute()

def process_exists(connection, process_type, process_id):
    return connection.exists(REDIS_KEY_ONE_INSTANCE % (process_type, process_id))

def stats_add_new_instance(connection, process_type, process_info):
    connection.zadd(REDIS_KEY_INSTANCES % process_type, time.time(), to_json(process_info))

# End Status and Stats methods


def write_one_output(connection, output_worker_type, output_worker_id, data):
    new_job_id = str(uuid.uuid4())
    data['jobs_ids'] = data['jobs_ids'] + [new_job_id]
    data['process']['worker_id'] = output_worker_id
    data['process']['worker_type'] = output_worker_type
    add_new_job_id(connection, data['process']['type'], data['process']['id'], output_worker_id, new_job_id)

    add_data_to_next_worker(connection, output_worker_type, to_json(data))

def prepare_write_output(data, process_data, worker_id):
    new_data = deepcopy(data)
    if not new_data['process'].get('workers'):
        new_data['process']['workers'] = []
    new_data['process']['workers'].append(worker_id)
    if not new_data.get('workers_output'):
        new_data['workers_output'] = {}
    new_data['workers_output'][worker_id] = process_data
    new_data['inputs'] = process_data
    return new_data

def write_outputs(connection, data, process_data, worker_script_data):
    data = prepare_write_output(data, process_data, data['process']['worker_id'])
    update_process(connection, data['process']['type'], data['process']['id'])
    pipe = connection.pipeline()
    workers_outputs_types = worker_script_data.get('__outputs_types', [])
    for i, output_worker_id in enumerate(worker_script_data.get('outputs', [])):
        write_one_output(connection, workers_outputs_types[i], output_worker_id, data)
    pipe.execute()

def write_finalized_job(data, process_data, worker_id, connection):
    data = prepare_write_output(data, process_data, worker_id)
    connection.zadd(
        REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS % (
            data['process']['type'], data['process']['id'], worker_id
        ),
        time.time(),
        to_json(data)
    )
    connection.lpush(
        REDIS_KEY_QUEUE_FINALIZED,
        to_json({
            'process_type': data['process']['type'],
            'process_id': data['process']['id'],
            'process_name': data['process']['name'],
            'worker_type': data['process']['worker_type'],
            'worker_id': worker_id
        })
    )

def start_process(connection, process_type, start_worker_type, start_worker_id, process_name, data):
    process_id = md5.new(process_name).hexdigest()

    exists_process_before = process_exists(connection, process_type, process_id)
    if not exists_process_before:
        process_data = {
            'name': process_name,
            'start_time': time.time(),
            'start_worker_id': start_worker_id,
            'start_worker_type': start_worker_type
        }
        update_process(connection, process_type, process_id, process_data)
    to_send_data = {
        'inputs': data,
        'process': {
            'type': process_type,
            'id': process_id,
            'name': process_name,
            'start_time': time.time(),
            'start_worker_id': start_worker_id,
            'start_worker_type': start_worker_type
        },
        'jobs_ids': [],
        'workers_output': {
            'initial': data
        },
        'workers': []
    }

    if not exists_process_before:
        stats_add_new_instance(connection, process_type, to_send_data['process'])

    write_one_output(connection, start_worker_type, start_worker_id, to_send_data)

    return process_id

def write_error_job(connection, worker_id, raw_data, error):
    metric_name = 'errors'
    try:
        data = json_loads(raw_data)
        if not data.get('workers_error'):
            data['workers_error'] = {}
        data['workers_error'][worker_id] = {
            'error': str(error), 'traceback': traceback.format_exc()
        }
        metric_name = REDIS_KEY_INSTANCE_WORKER_ERRORS % (
            data['process']['type'], data['process']['id'], worker_id
        )
    except Exception, e:
        data = {'workers_error': {}, 'raw_data': raw_data}
        data['workers_error'][worker_id] = {
            'error': 'cannot json_loads', 'traceback': traceback.format_exc()
        }
        metric_name = REDIS_KEY_INSTANCE_WORKER_ERRORS % (
            'unknown', 'unknown', worker_id
        )
    try:
        json_encoded = to_json(data)
    except Exception, e:
        json_encoded = to_json(convert_object_to_unicode(data))
    connection.zadd(metric_name, time.time(), json_encoded)


def default_main_inside(connection, worker_scripts, queue_message, process_func, *args, **kwargs):
    if queue_message is not None:
        raw_data = queue_message[1]
        try:
            data = get_job_data(raw_data)
            last_job_id = data['jobs_ids'][-1]
            worker_id = data['process']['worker_id']
            worker_script_data = get_worker_script_data(worker_scripts, data, data['process']['worker_id'])
            nodata = True
            for process_data in process_func(worker_script_data, data, *args, **kwargs):
                nodata = False
                if not process_data:
                    process_data = {}
                if process_data.get('__outputs'):
                    worker_script_data['outputs'] = process_data.get('__outputs', [])
                if len(worker_script_data.get('outputs', [])) == 0:
                    write_finalized_job(data, process_data, worker_id, connection)
                    continue
                write_outputs(connection, data, process_data, worker_script_data)
            if nodata:
                process_data = {}
                write_finalized_job(data, process_data, worker_id, connection)
            update_done_jobs(
                connection, data['process']['type'], data['process']['id'], worker_id, last_job_id
            )
        except Exception, e:
            worker_id = 'unknown'
            try:
                worker_id = data['process']['worker_id']
            except Exception, e:
                pass
            write_error_job(connection, worker_id, raw_data, e)
        # TODO: Manage if worker fails and message is lost

def pre_default_main_inside(
    connection, worker_scripts, worker_type, process_func, *args, **kwargs
):
    queue_message = connection.brpop([REDIS_KEY_QUEUE % worker_type,])
    default_main_inside(connection, worker_scripts, queue_message, process_func, *args, **kwargs)


def default_main(master_mind_url, worker_type, process_func, *args, **kwargs):
    worker_scripts, connection = prepare_worker(master_mind_url, worker_type)
    if os.environ.get('TRY_INPUT'):
        script_id = os.environ.get('SCRIPT_ID')
        raw_data = open(os.environ.get('TRY_INPUT')).read()
        data = get_job_data(raw_data)
        print list(process_func(worker_scripts.get(script_id), data, *args, **kwargs))
        return None
    print ' [*] Waiting for data. To exit press CTRL+C'
    while True:
        pre_default_main_inside(connection, worker_scripts, worker_type, process_func, *args, **kwargs)
