import urllib
from copy import deepcopy

from borealis.utils import base_request


class CollectionsApi(object):
    """See collections API documentation for descriptions of endpoints and
    their arguments.

    https://cwiki.apache.org/confluence/display/solr/Collections+API

    c = CollectionsApi('http://solr-server:8983/solr')
    c.cluster_status() # get cluster status
    c.add_replica({'collection': 'my_collection',
                   'shard': 'shard_name'})
    """
    def __init__(self, url):
        """Solr url in the form of http://hostname:port/solr"""
        self._url = "%s/admin/collections" % (url,)

    def _make_action_request(self, get_args, action):
        assert 'action' not in get_args
        get_args['action'] = action
        return get_args


def collections_api_request(url, get_args):
    """call the collections api with the provided get_args"""
    if get_args is None:
        get_args = {}

    get_args['wt'] = get_args.get('wt', 'json')
    assert get_args['wt'] == 'json'

    req = "%s?%s" % (url, urllib.urlencode(get_args, True))
    return base_request(req)


"""
The code below here dynamically creates methods on the CollectionsApi class
which correspond to specific actions that the Solr collections api supports.
"""
endpoints = (
    ('create', 'CREATE'),
    ('modify_collection', 'MODIFYCOLLECTION'),
    ('reload', 'RELOAD'),
    ('split_shard', 'SPLITSHARD'),
    ('create_shard', 'CREATESHARD'),
    ('delete_shard', 'DELETESHARD'),
    ('delete', 'DELETE'),
    ('delete_replica', 'DELETEREPLICA'),
    ('add_replica', 'ADDREPLICA'),
    ('cluster_prop', 'CLUSTERPROP'),
    ('migrate', 'MIGRATE'),
    ('add_role', 'ADDROLE'),
    ('remove_role', 'REMOVEROLE'),
    ('overseer_status', 'OVERSEERSTATUS'),
    ('cluster_status', 'CLUSTERSTATUS'),
    ('list', 'LIST'),
    ('add_replica_prop', 'ADDREPLICAPROP'),
    ('delete_replica_prop', 'DELETEREPLICAPROP'),
    ('balance_shard_unique', 'BALANCESHARDUNIQUE'),
    ('rebalance_leaders', 'REBALANCE_LEADERS'),
    ('force_leader', 'FORCE_LEADER'),
    ('migrate_state_format', 'MIGRATESTATEFORMAT'),
)


def _create_endpoint(name, action):
    def _endpoint_fn(self, get_args=None):
        if not get_args:
            get_args = {}
        return collections_api_request(
            self._url, self._make_action_request(get_args, action))
    setattr(CollectionsApi, name, _endpoint_fn)

for name, action in endpoints:
    _create_endpoint(name, action)

"""utility functions for deailing with the collections api"""


def replica_summary(cluster_status):
    """flatten the nested cluster status out into individual recplica dicts"""
    output = []
    collections = cluster_status['cluster']['collections']
    for col_name, col in collections.iteritems():
        for shard_name, shard in col['shards'].iteritems():
            for replica_name, replica_value in shard['replicas'].iteritems():
                o = deepcopy(replica_value)
                o.update({
                    'collection': col_name,
                    'shard': shard_name,
                    'replica': replica_name
                })
                output.append(o)
    return output
