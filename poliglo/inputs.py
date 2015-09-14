# -*- coding: utf-8 -*-
from poliglo.utils import select_dict_el, json_loads

def get_inputs(workflow_instance_data, worker_workflow_data):
    inputs = worker_workflow_data.get('default_inputs', {})
    select_inputs = select_dict_el(worker_workflow_data, 'before.select_inputs', {})
    for input_key, selector in select_inputs.iteritems():
        inputs[input_key] = select_dict_el(workflow_instance_data, selector)
    inputs.update(workflow_instance_data.get('inputs', {}))
    return inputs

def get_job_data(raw_data, encoding='utf-8'):
    data_to_loads = raw_data
    if not isinstance(raw_data, unicode):
        data_to_loads = raw_data.decode(encoding)
    return json_loads(data_to_loads)
