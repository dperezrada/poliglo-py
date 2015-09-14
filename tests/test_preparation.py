# -*- coding: utf-8 -*-
import mock
from unittest import TestCase

from redis.client import StrictRedis

import poliglo

from test_utils import mock_request, POLIGLO_SERVER_TEST_URL

class TestPreparations(TestCase):
    def setUp(self):
        self.config = {
            'REDIS_HOST': '1',
            'REDIS_PORT': '2',
            'REDIS_DB': '3',
        }

    @mock.patch('poliglo.preparation.StrictRedis')
    def test_get_connections(self, mock_redis):
        poliglo.preparation.get_connection(self.config)
        mock_redis.assert_has_calls([mock.call(host='1', port='2', db='3'),])

    @mock.patch('poliglo.utils.urllib2.urlopen')
    def test_get_config(self, mock_urlopen):
        # Mock the webserver to response all response with the content of the config
        mock_request(mock_urlopen, default_content=poliglo.utils.to_json(self.config))
        body = poliglo.preparation.get_config(POLIGLO_SERVER_TEST_URL, 'filter_worker')
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
            },
            'workflow2': {}
        }
        self.assertEqual(
            worker_workflows['workflow1']['filter_worker_1'],
            poliglo.preparation.get_worker_workflow_data(
                worker_workflows, workflow_instance_data, "filter_worker_1"
            )
        )
        self.assertEqual(
            worker_workflows['workflow1']['filter_worker_2'],
            poliglo.preparation.get_worker_workflow_data(
                worker_workflows, workflow_instance_data, "filter_worker_2"
            )
        )

    @mock.patch('poliglo.utils.urllib2.urlopen')
    def test_prepare_worker(self, mock_urlopen):
        meta_worker = 'meta_worker'
        mocked_urls = {}

        url = poliglo.variables.POLIGLO_SERVER_URL_WORKER_WORKFLOWS % (
            POLIGLO_SERVER_TEST_URL, meta_worker
            )

        mocked_urls[url] = {
            'body': poliglo.utils.to_json({
                'workflow1': {'default_inputs': {'version': 'v1'}},
                'workflow2': {'default_inputs': {'version': 'other'}}
            })
        }

        # Mock the webserver to return the data for the specific url
        mock_request(mock_urlopen, mocked_urls)
        worker_workflows, connection = poliglo.preparation.prepare_worker(
            POLIGLO_SERVER_TEST_URL, meta_worker
        )

        expected_worker_workflows = poliglo.utils.json_loads(mocked_urls[url]['body'])
        self.assertEqual(expected_worker_workflows, worker_workflows)
        self.assertIsInstance(connection, StrictRedis)
