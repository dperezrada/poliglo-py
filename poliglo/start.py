# -*- coding: utf-8 -*-
import md5
from time import time

from poliglo.status import workflow_instance_exists, update_workflow_instance, \
    stats_add_new_instance
from poliglo.outputs import write_one_output

def start_workflow_instance(
        connection, workflow, start_meta_worker,
        start_worker_id, workflow_instance_name, initial_data
    ):
    workflow_instance_id = md5.new(workflow_instance_name).hexdigest()

    exists_workflow_instance_before = workflow_instance_exists(
        connection, workflow, workflow_instance_id
    )
    workflow_instance_data = {
        'workflow': workflow,
        'id': workflow_instance_id,
        'name': workflow_instance_name,
        'creation_time': time(),
        'start_worker_id': start_worker_id,
        'start_meta_worker': start_meta_worker
    }

    if not exists_workflow_instance_before:
        update_workflow_instance(connection, workflow, workflow_instance_id, workflow_instance_data)

    to_send_data = {
        'inputs': initial_data,
        'workflow_instance': workflow_instance_data,
        'jobs_ids': [],
        'workers_output': {
            'initial': initial_data
        },
        'workers': []
    }

    if not exists_workflow_instance_before:
        stats_add_new_instance(connection, workflow, to_send_data['workflow_instance'])

    write_one_output(connection, start_meta_worker, start_worker_id, to_send_data)

    return workflow_instance_id
