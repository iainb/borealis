import urllib
import urllib2
import json
from borealis.utils import base_request


class UpdateApi(object):
    def __init__(self, url):
        self._url = url

    def insert_docs(self, collection, docs_or_doc):
        if not isinstance(docs_or_doc, list):
            docs = [docs_or_doc]
        else:
            docs = docs_or_doc

        return update_post_request(self._url, collection, {}, json.dumps(docs))

    def delete_docs_by_id(self, collection, doc_ids_or_id):
        if not isinstance(doc_ids_or_id, list):
            doc_ids = [doc_ids_or_id]
        else:
            doc_ids = doc_ids_or_id

        return update_post_request(
            self._url, collection, {}, json.dumps({'delete': doc_ids}))

    def delete_docs_by_query(self, collection, queries_or_query):
        if not isinstance(queries_or_query, list):
            queries = [queries_or_query]
        else:
            queries = queries_or_query

        solr_queries = [{'query': q} for q in queries]
        return update_post_request(
            self._url, collection, {}, json.dumps({'delete': solr_queries}))


def update_post_request(url, collection, get_args, post_data):
    get_args['wt'] = get_args.get('wt', 'json')
    assert get_args['wt'] == 'json'

    full_url = "%s/%s/update?%s" % (url, collection,
                                    urllib.urlencode(get_args, True))

    req = urllib2.Request(
        full_url, post_data, {'Content-Type': 'application/json'})

    return base_request(req)
