# -*- coding: utf-8 -*-
from unittest import TestCase
from mock import Mock, call, patch

import poliglo
from poliglo.utils import to_json, json_loads, select_dict_el
from poliglo.testing_utils import mock_request

MASTERMIND_TEST_URL = 'http://mastermind_url_test'

class TestInputs(TestCase):
    def setUp(self):
        self.specific_info = {
            "before": {
                "select_inputs": {
                    "email": "workers_output.get_emails_to_send.email",
                    "template_file": "workers_output.assign_variable.template_file"
                }
            },
            "outputs": ["worker1"]
        }
        self.data = {
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
        result = poliglo.get_inputs(self.data, self.specific_info)
        expected = {'email': 'test@test.com', 'template_file': '/tmp/testfile.handlebars'}
        self.assertEqual(expected, result)

    def test_before_select_inputs_not_overwrite_input(self):
        self.data['inputs'] = {'email': 'test_master@test.com'}
        result = poliglo.get_inputs(self.data, self.specific_info)
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
        body = poliglo.get_config(MASTERMIND_TEST_URL, 'worker_1')
        self.assertEqual(self.config, body)

    def test_get_specific_info(self):
        data = {'process': {'type': 'script1'}}
        worker_info = {
            'script1': {
                'default_inputs':{
                    'version': 'v1'
                }
            }, 'script2': {}
        }
        specific_info = poliglo.get_specific_info(worker_info, data)

        self.assertEqual(worker_info['script1'], specific_info)

    @patch('poliglo.utils.urllib2.urlopen')
    @patch('poliglo.redis.StrictRedis')
    def test_prepare_worker(self, mock_redis, mock_urlopen):
        worker_name = 'worker_name'
        mocked_urls = {}
        mocked_urls[MASTERMIND_TEST_URL+"/workers/"+worker_name+"/scripts"] = {
            'body': to_json({
                'script1': {'default_inputs': {'version': 'v1'}},
                'script2': {'default_inputs': {'version': 'other'}}
            })
        }
        mocked_urls[MASTERMIND_TEST_URL+"/workers/"+worker_name+"/config"] = {'body': to_json(self.config)}

        mock_request(mock_urlopen, mocked_urls)
        worker_info, connection = poliglo.prepare_worker(MASTERMIND_TEST_URL, 'worker_name')
        expected_worker_info = json_loads(mocked_urls[MASTERMIND_TEST_URL+"/workers/"+worker_name+"/scripts"]['body'])
        self.assertEqual(expected_worker_info, worker_info)
        # TODO: check connection is Redis type

class TestWriteOutputs(TestCase):
    def setUp(self):
        self.data = {
            'process': {
                'type': 'example_process',
                'id': '123',
            },
            'jobs_ids': ['5',]
        }
        self.process_data = {'message': 'hello'}
        self.worker_info = {
            'outputs': ['worker_2']
        }
        self.connection = Mock()

        self.worker_name = 'worker_name'

    def test_set_workers_output(self):
        poliglo.write_outputs(self.data, self.process_data, self.worker_info, self.connection, self.worker_name)
        self.assertEqual(self.process_data, self.data['workers_output'][self.worker_name])

    # TODO: Move this test to default_main_inside
    # def test_mark_job_as_done(self):
    #     poliglo.write_outputs(self.data, self.process_data, self.worker_info, self.connection, self.worker_name)
    #     self.connection.sadd.assert_any_call('scripts:example_process:processes:123:jobs_ids:done', '5')

    def test_add_new_pending_job(self):
        poliglo.write_outputs(self.data, self.process_data, self.worker_info, self.connection, self.worker_name)
        self.assertEqual(2, len(self.data['jobs_ids']))

class TestStartProcess(TestCase):
    def setUp(self):
        self.connection = Mock()
        self.process_type = 'example_process'
        self.start_worker = 'worker_1'
        self.process_name = 'example_process_instance1'
        self.data = {'template': '/tmp/lala'}
        poliglo.start_process(self.connection, self.process_type, self.start_worker, self.process_name, self.data)

    def test_inputs_set(self):
        _, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(self.data, received_data['inputs'])

    def test_set_jobs_ids(self):
        _, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(1, len(received_data['jobs_ids']))

    def test_inital_worker_output(self):
        _, raw_data = self.connection.lpush.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(self.data, received_data['workers_output']['initial'])

    def test_reuse_already_created_process(self):
        _, raw_data = self.connection.lpush.call_args[0]
        process_data = json_loads(raw_data)

        poliglo.start_process(self.connection, self.process_type, self.start_worker, self.process_name, self.data)
        _, raw_data = self.connection.lpush.call_args[0]
        process_data_2 = json_loads(raw_data)
        self.assertEqual(select_dict_el(process_data, 'process.id'), select_dict_el(process_data_2, 'process.id'))

    @patch('poliglo.update_process')
    def test_initial_create_process(self, mocked_update_process):
        connection = Mock()
        connection.exists.side_effect = [0,]
        poliglo.start_process(connection, self.process_type, self.start_worker, self.process_name, self.data)
        _, process_type, _, process_data = mocked_update_process.call_args[0]
        self.assertEqual(self.process_type, process_type)
        self.assertEqual(self.process_name, process_data.get('name'))

class TestWriteErrorJob(TestCase):
    def test_write_error(self):
        connection = Mock()
        worker_name = 'worker_1'
        raw_data = to_json({'process': {'type': 'process_1', 'id': '123'}})
        error = 'one error :('

        poliglo.write_error_job(connection, worker_name, raw_data, error)

        _, _, raw_data = connection.zadd.call_args[0]
        received_data = json_loads(raw_data)
        self.assertEqual(error, received_data['workers_error'][worker_name]['error'])

class TestDefaultMainInside(TestCase):
    def setUp(self):
        data = {'process': {'id': '1234', 'type': 'example_process'}, 'jobs_ids': ['35345']}
        self.connection = Mock()
        self.connection.brpop.side_effect = [(None, to_json(data))]
        self.worker_info = {
            'example_process': {
                'outputs': ['worker_2']
            }
        }
        self.worker_name = 'worker_1'

    @patch('poliglo.write_finalized_job')
    def test_no_output(self, write_finalized_job):
        def my_func(_, data):
            return [{'value': data['process']['id']}, ]
        worker_info = {}
        poliglo.default_main_inside(
            self.connection, worker_info, self.worker_name, my_func
        )
        process_data = write_finalized_job.call_args[0][1]
        self.assertEqual({'value': '1234'}, process_data)

    @patch('poliglo.write_outputs')
    def test_with_process_change_output(self, write_outputs_mock):
        def my_func(_, data):
            return [
                {'value': data['process']['id'], '__outputs': ['worker_3']},
            ]
        poliglo.default_main_inside(
            self.connection, self.worker_info, self.worker_name, my_func
        )
        specific_info = write_outputs_mock.call_args[0][2]
        self.assertEqual({'outputs': ['worker_3',]}, specific_info)

    @patch('poliglo.write_outputs')
    def test_with_no_process_data(self, write_outputs_mock):
        def my_func(_, data):
            return [
                None,
            ]
        poliglo.default_main_inside(
            self.connection, self.worker_info, self.worker_name, my_func
        )
        process_data = write_outputs_mock.call_args[0][1]
        self.assertEqual({}, process_data)

    @patch('poliglo.write_finalized_job')
    def test_with_no_data_returned(self, write_finalized_job):
        def my_func(_, data):
            return []
        poliglo.default_main_inside(
            self.connection, self.worker_info, self.worker_name, my_func
        )
        self.assertEqual(self.worker_name, write_finalized_job.call_args[0][2])

    @patch('poliglo.write_outputs')
    def test_with_default_args_and_kwargs(self, write_outputs_mock):
        def my_func(_, data, *args, **kwargs):
            return [
                {'args': args, 'kwargs': kwargs},
            ]
        poliglo.default_main_inside(
            self.connection, self.worker_info, self.worker_name, my_func,
            "prueba", "prueba2", conn1='conn1 value', conn2='conn2 value'
        )
        process_data = write_outputs_mock.call_args[0][1]
        self.assertEqual({
            'args': ('prueba', 'prueba2'),
            'kwargs': {'conn2': 'conn2 value', 'conn1': 'conn1 value'}
        }, process_data)

    @patch('poliglo.write_one_output')
    def test_set_inputs_in_data(self, write_one_output_mock):
        def my_func(_, data):
            return [
                {'name': 'this is a test'},
            ]
        poliglo.default_main_inside(
            self.connection, self.worker_info, self.worker_name, my_func
        )
        data = write_one_output_mock.call_args[0][2]
        self.assertEqual({'name': 'this is a test'}, data.get('inputs'))
