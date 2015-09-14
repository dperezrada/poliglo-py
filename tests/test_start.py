# -*- coding: utf-8 -*-
from unittest import TestCase
import mock

import poliglo
from poliglo.utils import json_loads, select_dict_el

POLIGLO_SERVER_TEST_URL = 'http://poliglo_server_url_test'

class TestStartWorkflowInstance(TestCase):
    def setUp(self):
        self.connection = mock.Mock()
        self.workflow = 'example_workflow_instance'
        self.start_worker_id = 'worker_1'
        self.start_meta_worker = 'worker'
        self.workflow_instance_name = 'example_workflow_instance_instance1'
        self.initial_data = {'template': '/tmp/lala'}
        poliglo.start.start_workflow_instance(
            self.connection, self.workflow, self.start_meta_worker, self.start_worker_id,
            self.workflow_instance_name, self.initial_data
        )

    def test_inputs_set(self):
        worker_queue, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(poliglo.variables.REDIS_KEY_QUEUE % self.start_meta_worker, worker_queue)
        self.assertEqual(self.initial_data, received_data['inputs'])

    def test_set_jobs_ids(self):
        _, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(1, len(received_data['jobs_ids']))

    def test_inital_worker_output(self):
        _, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(self.initial_data, received_data['workers_output']['initial'])

    def test_reuse_already_created_workflow_instance(self):
        _, raw_data = self.connection.lpush.call_args[0]
        workflow_instance_data = json_loads(raw_data)

        poliglo.start.start_workflow_instance(self.connection, self.workflow, self.start_meta_worker, self.start_worker_id, self.workflow_instance_name, self.initial_data)
        _, raw_data = self.connection.lpush.call_args[0]
        workflow_instance_data_2 = json_loads(raw_data)
        self.assertEqual(select_dict_el(workflow_instance_data, 'workflow_instance.id'), select_dict_el(workflow_instance_data_2, 'workflow_instance.id'))

    @mock.patch('poliglo.start.update_workflow_instance')
    def test_initial_create_workflow_instance(self, mocked_update_workflow_instance):
        connection = mock.Mock()
        connection.exists.side_effect = [0,]
        poliglo.start.start_workflow_instance(connection, self.workflow, self.start_meta_worker, self.start_worker_id, self.workflow_instance_name, self.initial_data)
        _, workflow, _, workflow_instance_data = mocked_update_workflow_instance.call_args[0]
        self.assertEqual(self.workflow, workflow)
        self.assertEqual(self.workflow_instance_name, workflow_instance_data.get('name'))
