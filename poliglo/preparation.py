# -*- coding: utf-8 -*-
from copy import deepcopy

from redis import StrictRedis

from poliglo.variables import POLIGLO_SERVER_URL_WORKER_CONFIG, POLIGLO_SERVER_URL_WORKER_WORKFLOWS
from poliglo.utils import make_request, json_loads, select_dict_el

def get_connection(worker_config):
    return StrictRedis(
        host=worker_config.get('REDIS_HOST'),
        port=worker_config.get('REDIS_PORT'),
        db=worker_config.get('REDIS_DB')
    )

def get_config(master_mind_url, meta_worker):
    _, _, content = make_request(POLIGLO_SERVER_URL_WORKER_CONFIG % (master_mind_url, meta_worker))
    worker_config = json_loads(content)
    return worker_config

def get_worker_workflow_data(worker_workflows, workflow_instance_data, worker_id):
    worker_workflow_data = worker_workflows.get(
        select_dict_el(workflow_instance_data, 'workflow_instance.workflow'), {}
    ).get(worker_id, {})

    if worker_workflow_data is None:
        worker_workflow_data = {}
    return deepcopy(worker_workflow_data)

def prepare_worker(master_mind_url, meta_worker):
    _, _, content = make_request(
        POLIGLO_SERVER_URL_WORKER_WORKFLOWS % (master_mind_url, meta_worker)
    )
    worker_workflows = json_loads(content)
    worker_config = get_config(master_mind_url, meta_worker)

    connection = get_connection(worker_config)
    return worker_workflows, connection
