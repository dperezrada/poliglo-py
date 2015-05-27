# -*- coding: utf-8 -*-
from mock import Mock

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
