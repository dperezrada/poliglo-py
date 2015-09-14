# -*- coding: utf-8 -*-
from unittest import TestCase
from mock import patch, Mock

from poliglo.utils import set_dict_el, select_dict_el, make_request

POLIGLO_SERVER_TEST_URL = 'http://poliglo_server_url_test'

class TestUtilsSelectAndSetDict(TestCase):
    def setUp(self):
        self.data = {'lala': {'hello': 'This is great'}}

    def test_not_exists_path(self):
        result = select_dict_el(self.data, 'not.exists')
        self.assertEqual(None, result)

    def test_not_exists_path_default_return(self):
        result = select_dict_el(self.data, 'not.exists', 'Default')
        self.assertEqual('Default', result)

    def test_get_correct_path(self):
        result = select_dict_el(self.data, 'lala.hello')
        self.assertEqual('This is great', result)

    def test_set_dict_el_correct_path(self):
        set_dict_el(self.data, 'lala.bye', 'See you')
        result = select_dict_el(self.data, 'lala.bye')
        self.assertEqual('See you', result)

    def test_set_dict_el_not_existing_path(self):
        set_dict_el(self.data, 'lala.hasta.la.vista.baby', 'See you')
        result = select_dict_el(self.data, 'lala.hasta.la.vista.baby')
        self.assertEqual('See you', result)


class TestUtilsMakeRequest(TestCase):
    @patch('poliglo.utils.urllib2.urlopen')
    def test_request(self, mock_urlopen):
        mocked_open = Mock()
        mocked_open.read.side_effect = ['<html>hola</html>',]
        mocked_open.code = 200
        mocked_open.close.side_effect = [None,]
        mock_urlopen.side_effect = [mocked_open,]
        code, _, body = make_request('http://example.com')
        self.assertEqual(200, code)
        self.assertEqual('<html>hola</html>', body)

def mock_request(
        mock_urlopen, specific_content=None, default_content=None,
        default_headers=None, default_status=200
    ):
    if not specific_content:
        specific_content = {}
    def open_side_effects(url):
        mocked_func = Mock()
        specific_url = specific_content.get(url, {})
        specific_url['body'] = specific_url.get('body') or default_content or '{}'
        specific_url['headers'] = specific_url.get('headers') or default_headers or \
            {'Content-Type': 'application/json'}
        specific_url['status'] = specific_url.get('status') or default_status or 200

        mocked_func.read.side_effect = [specific_url.get('body'),]
        mocked_func.headers.dict = specific_url.get('headers')
        mocked_func.code = specific_url.get('status')
        mocked_func.close.side_effect = [None,]
        return mocked_func
    mock_urlopen.side_effect = open_side_effects
