import urllib
import urllib2
import json

from borealis.utils import base_request


class SelectApi(object):
    def __init__(self, url):
        self._url = url

    def query(self, collection, args):
        if 'json.facet' in args:
            args['json.facet'] = json.dumps(args['json.facet'])
        return select_post_request(self._url, collection, args)

    def query_cursor_paging(self, collection, args):
        args['sort'] = args.get('sort', 'id asc')
        args['rows'] = args.get('rows', '2500')
        return query_cursor_paging(self._url, collection, args)


def query_cursor_paging(url, collection, args):
    """use solr's cursor paging to return very large result sets without
    using large ammounts of memory on the cluster.

    yields a page of documents at a time
    """
    prev_cursorMark = ""
    next_cursorMark = "*"
    while prev_cursorMark != next_cursorMark:
        args['cursorMark'] = next_cursorMark
        r = select_post_request(url, collection, args)

        prev_cursorMark = next_cursorMark
        next_cursorMark = r['nextCursorMark']
        yield r['response']['docs']


def select_post_request(url, collection, post_data):
    post_data['wt'] = post_data.get('wt', 'json')
    assert post_data['wt'] == 'json'

    # Return facets as JSON objects (which map directly to python dictionaries)
    # from the Solr docs this does have drawbacks:
    #
    # NamedList is represented as a JSON object. Although this is the simplest
    # mapping, a NamedList can have optional keys, repeated keys, and preserves
    # order. Using a JSON object (essentially a map or hash) for a NamedList
    # results in the loss of some information.
    post_data['json.nl'] = post_data.get('json.nl', 'map')

    full_url = "%s/%s/select?" % (url, collection)

    headers = {'Content-Type':
               'application/x-www-form-urlencoded; charset=utf-8'}
    post_data = urllib.urlencode(post_data, True)
    req = urllib2.Request(
        full_url, post_data, headers)

    return base_request(req)
