# -*- coding: utf-8 -*-
from unittest import TestCase
from mock import Mock, call, patch

from redis.client import StrictRedis

import poliglo
from poliglo.utils import to_json, json_loads, select_dict_el

POLIGLO_SERVER_TEST_URL = 'http://poliglo_server_url_test'

def mock_request(mock_urlopen, specific_content=None, default_content=None, default_headers=None, default_status=200):
    if not specific_content:
        specific_content = {}
    def open_side_effects(url):
        mocked_func = Mock()
        specific_url = specific_content.get(url, {})
        specific_url['body'] = specific_url.get('body') or default_content or '{}'
        specific_url['headers'] = specific_url.get('headers') or default_headers or {'Content-Type': 'application/json'}
        specific_url['status'] = specific_url.get('status') or default_status or 200

        mocked_func.read.side_effect = [specific_url.get('body'),]
        mocked_func.headers.dict = specific_url.get('headers')
        mocked_func.code = specific_url.get('status')
        mocked_func.close.side_effect = [None,]
        return mocked_func
    mock_urlopen.side_effect = open_side_effects


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
            }
        }
    def test_before_select_inputs(self):
        result = poliglo.get_inputs(self.workflow_instance_data, self.worker_workflow_data)
        expected = {'email': 'test@test.com', 'template_file': '/tmp/testfile.handlebars'}
        self.assertEqual(expected, result)

    def test_before_select_inputs_not_overwrite_input(self):
        self.workflow_instance_data['inputs'] = {'email': 'test_master@test.com'}
        result = poliglo.get_inputs(self.workflow_instance_data, self.worker_workflow_data)
        expected = {'email': 'test_master@test.com', 'template_file': '/tmp/testfile.handlebars'}
        self.assertEqual(expected, result)

class TestOthers(TestCase):
    def test_get_job_data_str(self):
        raw_data = to_json({'inputs': {'name': 'Daniel Pérez'}}).encode('utf-8')
        result = poliglo.get_job_data(raw_data, encoding='utf-8')
        self.assertEqual(u'Daniel Pérez', select_dict_el(result, 'inputs.name'))

    def test_get_job_data_unicode(self):
        raw_data = to_json({'inputs': {'name': 'Daniel Pérez'}})
        result = poliglo.get_job_data(raw_data, encoding='utf-8')
        self.assertEqual(u'Daniel Pérez', select_dict_el(result, 'inputs.name'))

class TestPreparations(TestCase):
    def setUp(self):
        self.config = {
            'REDIS_HOST': '1',
            'REDIS_PORT': '2',
            'REDIS_DB': '3',
        }

    @patch('poliglo.redis.StrictRedis')
    def test_get_connections(self, mock_redis):
        poliglo.get_connection(self.config)
        mock_redis.assert_has_calls([call(host='1', port='2', db='3'),])

    @patch('poliglo.utils.urllib2.urlopen')
    def test_get_config(self, mock_urlopen):
        mock_request(mock_urlopen, default_content=to_json(self.config))
        body = poliglo.get_config(POLIGLO_SERVER_TEST_URL, 'filter_worker')
        self.assertEqual(self.config, body)

    def test_get_worker_workflow_data(self):
        workflow_instance_data = {'workflow_instance': {'workflow': 'workflow1'}}
        worker_workflows = {
            'workflow1': {
                'filter_worker_1': {
                    'default_inputs':{
                        "name": "Juan"
                    }
                },
                'filter_worker_2':{
                    'default_inputs':{
                        "company": "Acme"
                    }
                }
            }, 'workflow2': {}
        }
        self.assertEqual(
            worker_workflows['workflow1']['filter_worker_1'],
            poliglo.get_worker_workflow_data(
                worker_workflows, workflow_instance_data, "filter_worker_1"
            )
        )
        self.assertEqual(
            worker_workflows['workflow1']['filter_worker_2'],
            poliglo.get_worker_workflow_data(
                worker_workflows, workflow_instance_data, "filter_worker_2"
            )
        )

    @patch('poliglo.utils.urllib2.urlopen')
    def test_prepare_worker(self, mock_urlopen):
        meta_worker = 'meta_worker_1'
        mocked_urls = {}

        url = poliglo.POLIGLO_SERVER_URL_WORKER_WORKFLOWS % (POLIGLO_SERVER_TEST_URL, meta_worker)

        mocked_urls[url] = {
            'body': to_json({
                'workflow1': {'default_inputs': {'version': 'v1'}},
                'workflow2': {'default_inputs': {'version': 'other'}}
            })
        }

        mock_request(mock_urlopen, mocked_urls)
        worker_workflows, connection = poliglo.prepare_worker(POLIGLO_SERVER_TEST_URL, meta_worker)

        expected_worker_workflows = json_loads(mocked_urls[url]['body'])
        self.assertEqual(expected_worker_workflows, worker_workflows)
        self.assertIsInstance(connection, StrictRedis)

class TestWriteOutputs(TestCase):
    def setUp(self):
        self.workflow_instance_data = {
            'workflow_instance': {
                'workflow': 'example_workflow_instance',
                'id': '123',
                'worker_id': 'worker_1'
            },
            'jobs_ids': ['5',]
        }
        self.worker_output_data = {'message': 'hello'}
        self.worker_workflow_data = {
            '__next_workers_types': ['write'],
            'next_workers': ['worker_2']
        }
        self.connection = Mock()

        self.worker_id = 'worker_1'

    @patch('poliglo.add_data_to_next_worker')
    def test_set_workers_output(self, mock_add_data_to_next_worker):
        poliglo.write_outputs(
            self.connection, self.workflow_instance_data,
            self.worker_output_data, self.worker_workflow_data
        )
        data_for_next_worker = json_loads(mock_add_data_to_next_worker.call_args[0][2])
        self.assertEqual(
            self.worker_output_data, data_for_next_worker['workers_output']['worker_1']
        )

    @patch('poliglo.add_data_to_next_worker')
    def test_set_workflow_instance_variables(self, mock_add_data_to_next_worker):
        poliglo.write_outputs(
            self.connection, self.workflow_instance_data,
            self.worker_output_data, self.worker_workflow_data
        )
        data_for_next_worker = json_loads(mock_add_data_to_next_worker.call_args[0][2])
        self.assertEqual([self.worker_id, ], data_for_next_worker['workflow_instance']['workers'])
        self.assertEqual(
            self.worker_workflow_data['next_workers'][0],
            data_for_next_worker['workflow_instance']['worker_id']
        )

    @patch('poliglo.add_data_to_next_worker')
    def test_add_new_pending_job(self, mock_add_data_to_next_worker):
        poliglo.write_outputs(
            self.connection, self.workflow_instance_data,
            self.worker_output_data, self.worker_workflow_data
        )
        data_for_next_worker = json_loads(mock_add_data_to_next_worker.call_args[0][2])
        self.assertEqual(2, len(data_for_next_worker['jobs_ids']))

class TestStartWorkflowInstance(TestCase):
    def setUp(self):
        self.connection = Mock()
        self.workflow = 'example_workflow_instance'
        self.start_worker_id = 'worker_1'
        self.start_meta_worker = 'worker'
        self.workflow_instance_name = 'example_workflow_instance_instance1'
        self.initial_data = {'template': '/tmp/lala'}
        poliglo.start_workflow_instance(self.connection, self.workflow, self.start_meta_worker, self.start_worker_id, self.workflow_instance_name, self.initial_data)

    def test_inputs_set(self):
        worker_queue, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(poliglo.REDIS_KEY_QUEUE % self.start_meta_worker, worker_queue)
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

        poliglo.start_workflow_instance(self.connection, self.workflow, self.start_meta_worker, self.start_worker_id, self.workflow_instance_name, self.initial_data)
        _, raw_data = self.connection.lpush.call_args[0]
        workflow_instance_data_2 = json_loads(raw_data)
        self.assertEqual(select_dict_el(workflow_instance_data, 'workflow_instance.id'), select_dict_el(workflow_instance_data_2, 'workflow_instance.id'))

    @patch('poliglo.update_workflow_instance')
    def test_initial_create_workflow_instance(self, mocked_update_workflow_instance):
        connection = Mock()
        connection.exists.side_effect = [0,]
        poliglo.start_workflow_instance(connection, self.workflow, self.start_meta_worker, self.start_worker_id, self.workflow_instance_name, self.initial_data)
        _, workflow, _, workflow_instance_data = mocked_update_workflow_instance.call_args[0]
        self.assertEqual(self.workflow, workflow)
        self.assertEqual(self.workflow_instance_name, workflow_instance_data.get('name'))

class TestWriteErrorJob(TestCase):
    def test_write_error(self):
        connection = Mock()
        worker_id = 'worker_1'
        workflow_instance_data = to_json({'workflow_instance': {'workflow': 'workflow_instance_1', 'id': '123'}})
        error = 'one error :('

        poliglo.write_error_job(connection, worker_id, workflow_instance_data, error)

        _, _, workflow_instance_received = connection.zadd.call_args[0]
        received_data = json_loads(workflow_instance_received)
        self.assertEqual(error, received_data['workers_error'][worker_id]['error'])

class TestDefaultMainInside(TestCase):
    def setUp(self):
        workflow_instance_data = {
            'workflow_instance': {
                'id': '1234',
                'workflow': 'example_workflow_instance',
                'worker_id': 'worker_1'
            }, 'jobs_ids': ['35345']
        }
        self.connection = Mock()
        self.connection.brpop.side_effect = [(None, to_json(workflow_instance_data))]
        self.worker_workflows = {
            'example_workflow_instance': {
                'worker_1': {
                    '__next_workers_types': ['worker'],
                    'next_workers': ['worker_2']
                }
            }
        }
        self.meta_worker = 'worker_1_test'

    @patch('poliglo.write_finalized_job')
    def test_no_output(self, write_finalized_job):
        def my_func(_, data):
            return [{'value': data['workflow_instance']['id']}, ]
        worker_workflows = {}
        poliglo.pre_default_main_inside(
            self.connection, worker_workflows, self.meta_worker, my_func
        )
        workflow_instance_data = write_finalized_job.call_args[0][1]
        self.assertEqual({'value': '1234'}, workflow_instance_data)


    @patch('poliglo.write_outputs')
    def test_with_workflow_instance_change_output(self, write_outputs_mock):
        def my_func(_, data):
            return [
                {'value': data['workflow_instance']['id'], '__next_workers': ['worker_3']},
            ]
        poliglo.pre_default_main_inside(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        worker_workflow_data = write_outputs_mock.call_args[0][3]
        self.assertEqual(['worker_3',], worker_workflow_data['next_workers'])

    @patch('poliglo.write_outputs')
    def test_with_no_workflow_instance_data(self, write_outputs_mock):
        def my_func(_, data):
            return [
                None,
            ]
        poliglo.pre_default_main_inside(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        workflow_instance_data = write_outputs_mock.call_args[0][2]
        self.assertEqual({}, workflow_instance_data)

    @patch('poliglo.write_finalized_job')
    def test_with_no_data_returned(self, write_finalized_job):
        def my_func(_, data):
            return []
        poliglo.pre_default_main_inside(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        self.assertEqual('worker_1', write_finalized_job.call_args[0][2])

    @patch('poliglo.write_outputs')
    def test_with_default_args_and_kwargs(self, write_outputs_mock):
        def my_func(_, data, *args, **kwargs):
            return [
                {'args': args, 'kwargs': kwargs},
            ]
        poliglo.pre_default_main_inside(
            self.connection, self.worker_workflows, self.meta_worker, my_func,
            "prueba", "prueba2", conn1='conn1 value', conn2='conn2 value'
        )
        workflow_instance_data = write_outputs_mock.call_args[0][2]
        self.assertEqual({
            'args': ('prueba', 'prueba2'),
            'kwargs': {'conn2': 'conn2 value', 'conn1': 'conn1 value'}
        }, workflow_instance_data)

    @patch('poliglo.write_one_output')
    def test_set_inputs_in_data(self, write_one_output_mock):
        def my_func(_, data):
            return [
                {'name': 'this is a test'},
            ]
        poliglo.pre_default_main_inside(
            self.connection, self.worker_workflows, self.meta_worker, my_func
        )
        data = write_one_output_mock.call_args[0][3]
        self.assertEqual({'name': 'this is a test'}, data.get('inputs'))

# class TestWriteWorkerJobTimming(TestCase):
#     def setUp(self):
#         self.data = {
#             'workflow_instance': {
#                 'workflow': 'example_workflow_instance',
#                 'id': '123',
#                 'worker_id': 'worker_1'
#             },
#             'jobs_ids': ['5',]
#         }
#         self.workflow_instance_data = {'message': 'hello'}
#         self.worker_workflow_data = {
#             '__next_workers_types': ['write'],
#             'next_workers': ['worker_2']
#         }
#         self.connection = Mock()

#         self.worker_id = 'worker_1'

#     @patch('poliglo.add_data_to_next_worker')
#     def test_set_workers_output(self, mock_add_data_to_next_worker):
#         poliglo.write_outputs(self.connection, self.data, self.workflow_instance_data, self.worker_workflow_data)
#         print mock_add_data_to_next_worker.call_args
#         data_for_next_worker = json_loads(mock_add_data_to_next_worker.call_args[0][2])
#         self.assertEqual(self.workflow_instance_data, data_for_next_worker['workers_output']['worker_1'])
