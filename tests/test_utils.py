# -*- coding: utf-8 -*-
from unittest import TestCase
from mock import patch, Mock

from poliglo.utils import set_dict_el, select_dict_el, make_request
from poliglo.testing_utils import mock_request

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
        mock_request(mock_urlopen, default_content='<html>hola</html>', default_status=200, default_headers={'Content-Type': 'text/html'})
        code, headers, body = make_request('http://example.com')
        self.assertEqual(200, code)
        self.assertEqual({'Content-Type': 'text/html'}, headers)
        self.assertEqual('<html>hola</html>', body)

