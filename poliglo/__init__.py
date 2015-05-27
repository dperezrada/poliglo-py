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
REDIS_KEY_INSTANCES = 'scripts:%s:processes'
REDIS_KEY_ONE_INSTANCE = "scripts:%s:processes:%s"
REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS = "scripts:%s:processes:%s:workers:%s:finalized"
REDIS_KEY_INSTANCE_WORKER_JOBS = "scripts:%s:processes:%s:workers:%s:jobs_ids:%s"
REDIS_KEY_INSTANCE_WORKER_ERRORS = "scripts:%s:processes:%s:workers:%s:errors"
REDIS_KEY_INSTANCE_WORKER_DISCARDED = "scripts:%s:processes:%s:workers:%s:discarded"

MASTER_MIND_URL_WORKER_CONFIG = "%s/workers/%s/config"
MASTER_MIND_URL_WORKER_SCRIPTS = "%s/workers/%s/scripts"

# Start Preparation methods
def get_connection(worker_config, target='redis'):
    if target == 'redis':
        return redis.StrictRedis(
            host=worker_config.get('REDIS_HOST', '127.0.0.1'),
            port=worker_config.get('REDIS_PORT', '6379'),
            db=worker_config.get('REDIS_DB', '0')
        )

def get_config(master_mind_url, worker_name):
    _, _, content = make_request(MASTER_MIND_URL_WORKER_CONFIG % (master_mind_url, worker_name))
    worker_config = json_loads(content)
    return worker_config

def get_specific_info(worker_info, data):
    specific_info = worker_info.get(select_dict_el(data, 'process.type'))
    if specific_info is None:
        specific_info = {}
    # TODO: check if deepcopy this is only needed for testing purpose
    return deepcopy(specific_info)

def prepare_worker(master_mind_url, worker_name):
    _, _, content = make_request(MASTER_MIND_URL_WORKER_SCRIPTS % (master_mind_url, worker_name))
    worker_info = json_loads(content)
    worker_config = get_config(master_mind_url, worker_name)

    connection = get_connection(worker_config)
    return worker_info, connection

def get_inputs(data, specific_info):
    inputs = specific_info.get('default_inputs', {})
    select_inputs = select_dict_el(specific_info, 'before.select_inputs', {})
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

def append_output_to_next_worker(connection, output, raw_data):
    connection.lpush(REDIS_KEY_QUEUE % output, raw_data)

def update_done_jobs(connection, process_type, instance_id, worker_name, job_id):
    connection.sadd(
        REDIS_KEY_INSTANCE_WORKER_JOBS % (process_type, instance_id, worker_name, 'done'),
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


def write_one_output(connection, output, data):
    new_job_id = str(uuid.uuid4())
    data['jobs_ids'] = data['jobs_ids'] + [new_job_id]
    add_new_job_id(connection, data['process']['type'], data['process']['id'], output, new_job_id)

    append_output_to_next_worker(connection, output, to_json(data))

def prepare_write_output(data, process_data, worker_name):
    data['process']['last_worker'] = worker_name

    if not data.get('workers_output'):
        data['workers_output'] = {}
    data['workers_output'][worker_name] = process_data
    data['inputs'] = process_data
    return data

def write_outputs(data, process_data, worker_info, connection, worker_name):
    data = prepare_write_output(data, process_data, worker_name)
    update_process(connection, data['process']['type'], data['process']['id'])
    pipe = connection.pipeline()
    for output in worker_info.get('outputs', []):
        write_one_output(pipe, output, data)
    pipe.execute()

def write_finalized_job(data, process_data, worker_name, connection):
    data = prepare_write_output(data, process_data, worker_name)
    connection.zadd(
        REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS % (
            data['process']['type'], data['process']['id'], worker_name
        ),
        time.time(),
        to_json(data)
    )

def start_process(connection, process_type, start_worker, process_name, data):
    process_id = md5.new(process_name).hexdigest()
    job_id = str(uuid.uuid4())

    add_new_job_id(connection, process_type, process_id, start_worker, job_id)

    exists_process_before = process_exists(connection, process_type, process_id)
    if not exists_process_before:
        process_data = {
            'name': process_name,
            'start_time': time.time(),
            'start_worker': start_worker
        }
        update_process(connection, process_type, process_id, process_data)
    to_send_data = {
        'inputs': data,
        'process': {
            'type': process_type,
            'id': process_id,
            'name': process_name,
            'start_time': time.time(),
            'start_worker': start_worker
        },
        'jobs_ids': [job_id,],
        'workers_output': {
            'initial': data
        },
        'last_worker': ''
    }
    append_output_to_next_worker(connection, start_worker, to_json(to_send_data))

    if not exists_process_before:
        stats_add_new_instance(connection, process_type, to_send_data['process'])

    return process_id

def write_error_job(connection, worker_name, raw_data, error):
    metric_name = 'errors'
    try:
        data = json_loads(raw_data)
        if not data.get('workers_error'):
            data['workers_error'] = {}
        data['workers_error'][worker_name] = {
            'error': str(error), 'traceback': traceback.format_exc()
        }
        metric_name = REDIS_KEY_INSTANCE_WORKER_ERRORS % (
            data['process']['type'], data['process']['id'], worker_name
        )
    except Exception, e:
        data = {'workers_error': {}, 'raw_data': raw_data}
        data['workers_error'][worker_name] = {
            'error': 'cannot json_loads', 'traceback': traceback.format_exc()
        }
        metric_name = REDIS_KEY_INSTANCE_WORKER_ERRORS % (
            'unknown', 'unknown', worker_name
        )
    try:
        json_encoded = to_json(data)
    except Exception, e:
        json_encoded = to_json(convert_object_to_unicode(data))
    connection.zadd(metric_name, time.time(), to_json(data))


def default_main_inside(connection, worker_info, worker_name, process_func, *args, **kwargs):
    _, raw_data = connection.brpop([REDIS_KEY_QUEUE % worker_name,])
    try:

        data = get_job_data(raw_data)
        last_job_id = data['jobs_ids'][-1]
        specific_info = get_specific_info(worker_info, data)
        nodata = True
        for process_data in process_func(specific_info, data, *args, **kwargs):
            nodata = False
            if not process_data:
                process_data = {}
            if process_data.get('__outputs'):
                specific_info['outputs'] = process_data.get('__outputs', [])
            if len(specific_info.get('outputs', [])) == 0:
                write_finalized_job(data, process_data, worker_name, connection)
                continue
            write_outputs(data, process_data, specific_info, connection, worker_name)
        if nodata:
            process_data = {}
            write_finalized_job(data, process_data, worker_name, connection)
        update_done_jobs(
            connection, data['process']['type'], data['process']['id'], worker_name, last_job_id
        )
    except Exception, e:
        write_error_job(connection, worker_name, raw_data, e)
    # TODO: Manage if worker fails and message is lost

def default_main(master_mind_url, worker_name, process_func, *args, **kwargs):
    worker_info, connection = prepare_worker(master_mind_url, worker_name)
    if os.environ.get('TRY_INPUT'):
        script_id = os.environ.get('SCRIPT_ID')
        raw_data = open(os.environ.get('TRY_INPUT')).read()
        data = get_job_data(raw_data)
        print list(process_func(worker_info.get(script_id), data, *args, **kwargs))
        return None
    print ' [*] Waiting for data. To exit press CTRL+C'
    while True:
        default_main_inside(connection, worker_info, worker_name, process_func, *args, **kwargs)
