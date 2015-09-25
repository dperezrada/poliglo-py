# -*- coding: utf-8 -*-
from unittest import TestCase

import poliglo

class TestInputs(TestCase):
    def setUp(self):
        self.worker_workflow_data = {
            "before": {
                "select_inputs": {
                    "email": "workers_output.get_emails_to_send.email",
                    "template_file": "workers_output.assign_variable.template_file"
                }
            },
            "next_workers": ["worker1"]
        }
        self.workflow_instance_data = {
            'workers_output': {
                'get_emails_to_send': {
                    'email': 'test@test.com'
                },
                'assign_variable': {
                    'template_file': '/tmp/testfile.handlebars'
                }
            },
            'inputs': {}
        }
    def test_before_select_inputs(self):
        result = poliglo.inputs.get_inputs(self.workflow_instance_data, self.worker_workflow_data)
        expected = {'email': 'test@test.com', 'template_file': '/tmp/testfile.handlebars'}
        self.assertEqual(expected, result)

    def test_before_select_inputs_not_overwrite_input(self):
        self.workflow_instance_data['inputs'] = {'email': 'test_master@test.com'}
        result = poliglo.inputs.get_inputs(self.workflow_instance_data, self.worker_workflow_data)
        expected = {'email': 'test_master@test.com', 'template_file': '/tmp/testfile.handlebars'}
        self.assertEqual(expected, result)

class TestJobData(TestCase):
    def test_get_job_data_str(self):
        raw_data = poliglo.utils.to_json({'inputs': {'name': 'Daniel Pérez'}}).encode('utf-8')
        result = poliglo.inputs.get_job_data(raw_data, encoding='utf-8')
        self.assertEqual(u'Daniel Pérez', poliglo.utils.select_dict_el(result, 'inputs.name'))

    def test_get_job_data_unicode(self):
        raw_data = poliglo.utils.to_json({'inputs': {'name': 'Daniel Pérez'}})
        result = poliglo.inputs.get_job_data(raw_data, encoding='utf-8')
        self.assertEqual(u'Daniel Pérez', poliglo.utils.select_dict_el(result, 'inputs.name'))
