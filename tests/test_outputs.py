# -*- coding: utf-8 -*-
import mock
from unittest import TestCase

import poliglo

class TestWriteOutputs(TestCase):
    def setUp(self):
        self.workflow_instance_data = {
            'workflow_instance': {
                'workflow': 'example_workflow_instance',
                'id': '123',
                'meta_worker': 'worker',
                'worker_id': 'worker_1'
            },
            'jobs_ids': ['5',]
        }
        self.worker_output_data = {'message': 'hello'}
        self.worker_workflow_data = {
            '__next_workers_types': ['write'],
            'next_workers': ['worker_2']
        }
        self.connection = mock.Mock()

        self.worker_id = 'worker_1'

    @mock.patch('poliglo.outputs.add_data_to_next_worker')
    def test_set_workers_output(self, mock_add_data_to_next_worker):
        poliglo.outputs.write_outputs(
            self.connection, self.workflow_instance_data,
            self.worker_output_data, self.worker_workflow_data
        )
        data_for_next_worker = poliglo.utils.json_loads(
            mock_add_data_to_next_worker.call_args[0][2]
        )
        self.assertEqual(
            self.worker_output_data, data_for_next_worker['workers_output']['worker_1']
        )

    @mock.patch('poliglo.outputs.add_data_to_next_worker')
    def test_set_workflow_instance_variables(self, mock_add_data_to_next_worker):
        poliglo.outputs.write_outputs(
            self.connection, self.workflow_instance_data,
            self.worker_output_data, self.worker_workflow_data
        )
        data_for_next_worker = poliglo.utils.json_loads(
            mock_add_data_to_next_worker.call_args[0][2]
        )
        self.assertEqual([self.worker_id, ], data_for_next_worker['workflow_instance']['workers'])
        self.assertEqual(
            self.worker_workflow_data['next_workers'][0],
            data_for_next_worker['workflow_instance']['worker_id']
        )

    @mock.patch('poliglo.outputs.add_data_to_next_worker')
    def test_add_new_pending_job(self, mock_add_data_to_next_worker):
        poliglo.outputs.write_outputs(
            self.connection, self.workflow_instance_data,
            self.worker_output_data, self.worker_workflow_data
        )
        data_for_next_worker = poliglo.utils.json_loads(
            mock_add_data_to_next_worker.call_args[0][2]
        )
        self.assertEqual(2, len(data_for_next_worker['jobs_ids']))

class TestWriteErrorJob(TestCase):
    def test_write_error(self):
        connection = mock.Mock()
        worker_id = 'worker_1'
        workflow_instance_data = poliglo.utils.to_json(
            {'workflow_instance': {'workflow': 'workflow_instance_1', 'id': '123'}}
        )
        error = 'one error :('

        poliglo.outputs.write_error_job(connection, worker_id, workflow_instance_data, error)

        _, _, workflow_instance_received = connection.zadd.call_args[0]
        received_data = poliglo.utils.json_loads(workflow_instance_received)
        self.assertEqual(error, received_data['workers_error'][worker_id]['error'])
