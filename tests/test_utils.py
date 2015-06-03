# -*- coding: utf-8 -*-
from unittest import TestCase
from mock import patch, Mock

from poliglo.utils import set_dict_el, select_dict_el, make_request

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
