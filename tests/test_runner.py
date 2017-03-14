# -*- coding: utf-8 -*-
import mock
from unittest import TestCase

import poliglo

class TestDefaultMainInside(TestCase):
    def setUp(self):
        workflow_instance_data = {
            'workflow_instance': {
                'id': '1234',
                'meta_worker': 'worker',
                'workflow': 'example_workflow_instance',
                'worker_id': 'worker_1'
            }, 'jobs_ids': ['35345']
        }
        self.connection = mock.Mock()
        workflow_data = poliglo.utils.to_json(workflow_instance_data)
        self.connection.brpoplpush.side_effect = [workflow_data, workflow_data]
        self.worker_workflows = {
            'example_workflow_instance': {
                'worker_1': {
                    '__next_workers_types': ['worker'],
                    'next_workers': ['worker_2']
                }
            }
        }
        self.meta_worker = 'worker_1_test'

    @mock.patch('poliglo.runner.write_finalized_job')
    def test_no_output_defined_in_workflow(self, write_finalized_job):
        def my_func(_, workflow_instance_data):
            return [{'value': workflow_instance_data['workflow_instance']['id']}, ]
        worker_workflows = {}
        poliglo.runner.default_main_inside_wrapper(
            self.connection, worker_workflows, self.meta_worker, my_func
        )
        workflow_instance_data = write_finalized_job.call_args[0][1]
        self.assertEqual({'value': '1234'}, workflow_instance_data)


    @mock.patch('poliglo.runner.write_outputs')
    def test_with_workflow_instance_change_output(self, write_outputs_mock):
        def my_func(_, workflow_instance_data):
            return [
                {'value': workflow_instance_data['workflow_instance']['id'],
                '__next_workers': ['worker_3']},
            ]
        poliglo.runner.default_main_inside_wrapper(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        worker_workflow_data = write_outputs_mock.call_args[0][3]
        self.assertEqual(['worker_3',], worker_workflow_data['next_workers'])

    @mock.patch('poliglo.runner.write_outputs')
    def test_with_no_workflow_instance_data(self, write_outputs_mock):
        def my_func(_, workflow_instance_data):
            return [
                None,
            ]
        poliglo.runner.default_main_inside_wrapper(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        workflow_instance_data = write_outputs_mock.call_args[0][2]
        self.assertEqual({}, workflow_instance_data)

    @mock.patch('poliglo.runner.write_finalized_job')
    def test_with_no_data_returned(self, write_finalized_job):
        def my_func(_, workflow_instance_data):
            return []
        poliglo.runner.default_main_inside_wrapper(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        self.assertEqual('worker_1', write_finalized_job.call_args[0][2])

    @mock.patch('poliglo.runner.write_outputs')
    def test_with_default_args_and_kwargs(self, write_outputs_mock):
        def my_func(_, workflow_instance_data, *args, **kwargs):
            return [
                {'args': args, 'kwargs': kwargs},
            ]
        poliglo.runner.default_main_inside_wrapper(
            self.connection, self.worker_workflows, self.meta_worker, my_func,
            "prueba", "prueba2", conn1='conn1 value', conn2='conn2 value'
        )
        workflow_instance_data = write_outputs_mock.call_args[0][2]
        self.assertEqual({
            'args': ('prueba', 'prueba2'),
            'kwargs': {'conn2': 'conn2 value', 'conn1': 'conn1 value'}
        }, workflow_instance_data)

    @mock.patch('poliglo.outputs.write_one_output')
    def test_set_inputs_in_data(self, write_one_output_mock):
        def my_func(_, workflow_instance_data):
            return [
                {'name': 'this is a test'},
            ]
        poliglo.runner.default_main_inside_wrapper(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        workflow_instance_data = write_one_output_mock.call_args[0][3]
        self.assertEqual({'name': 'this is a test'}, workflow_instance_data.get('inputs'))

    @mock.patch('poliglo.outputs.write_one_output')
    def test_set_worker_times(self, write_one_output_mock):
        def my_func(_, workflow_instance_data):
            return [
                {'name': 'this is a test'},
            ]
        poliglo.runner.default_main_inside_wrapper(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        key, timming = self.connection.lpush.call_args[0]
        self.assertEqual(
            'workflows:example_workflow_instance:workflow_instances:1234:workers:worker_1:timing',
            key
        )
        self.assertGreaterEqual(0.01, timming)
