# -*- coding: utf-8 -*-
import urllib2
import json
import collections

def set_dict_el(data, key_expr, value):
    curr_el = data
    keys = key_expr.split('.')[:-1]
    final_key = key_expr.split('.')[-1]
    for key in keys:
        if not curr_el.get(key):
            curr_el[key] = {}
        curr_el = curr_el[key]
    curr_el[final_key] = value

def select_dict_el(data, key_expr, default=None):
    curr_el = data
    for key in key_expr.split('.'):
        if not curr_el.get(key):
            return default
        curr_el = curr_el[key]
    return curr_el

def make_request(url, simulate_browser=None):
    escaped_url = urllib2.quote(url, safe="%/:=&?~#+!$,;'@()*[]")
    if simulate_browser:
        opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.112 Safari/534.30')]
        req = opener.open(escaped_url)
    else:
        req = urllib2.urlopen(escaped_url)
    body = req.read()
    headers = req.headers.dict
    status = req.code
    req.close()
    return (status, headers, body)

def to_json(data, encoding='utf-8', ensure_ascii=False):
    json_encoded = json.dumps(data, ensure_ascii=ensure_ascii, encoding=encoding)
    if isinstance(json_encoded, str):
        return json_encoded.decode(encoding)
    return json_encoded


def json_loads(raw_data, encoding='utf-8'):
    return json.loads(raw_data, encoding=encoding)


# Based on http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
def convert_object_to_unicode(data, encoding='utf-8'):
    if isinstance(data, basestring):
        return data.decode(encoding, encoding="ignore")
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_to_unicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_to_unicode, data))
    else:
        return data
