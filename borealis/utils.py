from datetime import datetime, tzinfo, timedelta
import json
import urllib2

from borealis.exceptions import SolrHTTPException


class UTC(tzinfo):
    """A UTC timezone class for parsing Solr datetime strings"""
    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)

utc = UTC()


def utc_to_string(value):
    """Convert datetimes to the subset of ISO 8601 that Solr expects."""
    value = value.astimezone(utc).isoformat()
    if '+' in value:
        value = value.split('+')[0]
    value += 'Z'
    return value


def utc_from_string(value):
    """Parse a string representing an ISO 8601 date.
    Note: this doesn't process the entire ISO 8601 standard,
    onle the specific format Solr promises to generate.
    """
    try:
        if not value.endswith('Z') and value[10] == 'T':
            raise ValueError(value)
        year = int(value[0:4])
        month = int(value[5:7])
        day = int(value[8:10])
        hour = int(value[11:13])
        minute = int(value[14:16])
        microseconds = int(float(value[17:-1]) * 1000000.0)
        second, microsecond = divmod(microseconds, 1000000)
        return datetime(
            year, month, day, hour,
            minute, second, microsecond, utc)
    except ValueError:
        raise ValueError("'%s' is not a valid ISO 8601 Solr date" % value)


def base_request(urllib_req):
    """make a request to a solr api endpoint using urllib2, handle errors
    and attempt to parse the returned JSON response.
    """
    try:
        response = urllib2.urlopen(urllib_req)
    except urllib2.HTTPError as e:
        raise SolrHTTPException(e)

    response = response.read()

    return json.loads(response)
