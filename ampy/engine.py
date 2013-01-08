#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Connects to an AMP database and interacts with it
"""

import random
import time
import urllib2
import json
import httplib
import sys


class Connection(object):
    """ Class that is used to query the AMP dataset. Queries will return a
        Result object that can be iterated on
    """

    def __init__(self):
        """ Initialises an AMP connection """
        self._connect()
        self.urlbase = "http://erg.wand.net.nz/amp/testdata2/json"
        self.apikey = "cathyisastud"

    def _connect(self):
        """ Connects to AMP """
        pass

    def get_sources(self, start=None, end=None):
        """ Get all source monitors """
        # TODO Filter results based on having specific test data available?
        return self._get_sources(start, end)

    def get_destinations(self, src=None, start=None, end=None):
        """ Get all destinations from the given source """
        # If no source is given then find all possible destinations
        if src is None:
            destinations = set()
            # TODO This is not very efficient, but will be improved by the DB.
            # TODO It also fetches a whole lot of destinations that are only the
            # target of a few special tests, or ones that are now deprecated
            # and have no recent data.
            # TODO Filter results based on having specific test data available?
            for src in self._get_sources(start, end):
                for dst in self._get_destinations(src, start, end):
                    destinations.add(dst)
            return Result(list(destinations))
        return self._get_destinations(src, start, end)

    def get_tests(self, src, dst, start=None, end=None):
        """ Fetches all tests that are performed between src and dst """
        # TODO Deal with any of src or dst not being set and instead return
        # all tests to or from a host.
        return self._get_tests(src, dst, start, end)

    def get(self, src=None, dst=None, test=None, subtype=None, start=None, 
            end=None, binsize=60, rand=False):
        """ Fetches data from the connection, returning a Result object
        
            Keyword arguments:
            src -- source to get data for, or None to fetch all sources
            dst -- dest to get data for, or None to fetch all valid dests
            test -- test to get data for, or None to fetch all valid tests
            subtype -- subtype to get data for, or None to fetch all valid ones
            start -- timestamp for the start of the period to fetch data for
            end -- timestamp for the end of the period to fetch data for
            binsize -- number of seconds worth of data to bin
            rand -- if true will generate random data rather than real data
        """

        if src is None:
            # Pass through other args so we can do smart filtering?
            return self._get_sources(start, end)

        if dst is None:
            # Pass through other args so we can do smart filtering?
            return self._get_destinations(src, start, end)
        
        if test is None:
            # Pass through other args so we can do smart filtering?
            return self._get_tests(src, dst, start, end)

        if test is None:
            # Pass through other args so we can do smart filtering?
            return self._get_tests(src, dst, start, end)

        if subtype is None:
            return self._get_subtypes(src, dst, test, start, end)

        # FIXME: Consider limiting maximum durations based on binsize
        # if end is not set then assume "now".
        if end is None:
            end = int(time.time())

        # If start is not set then assume 5 minutes before the end.
        if start is None:
            start = end - (60*5)

        return self._get_data(src, dst, test, subtype, start, end, binsize, rand)

    def _get_json(self, url, expected, binsize=60):
        """ Query the old REST API to get data """
        # TODO Don't query the old API, query the new one that will be written!
        try:
            url = "%s/%s;api_key=%s&stat=all&binsize=%d" % (
                    self.urlbase, url, self.apikey, binsize)
            request = urllib2.Request(url)
            response = urllib2.urlopen(request, None, 30)
        except (urllib2.URLError, httplib.BadStatusLine):
            print >> sys.stderr, "error fetching data from %s" % url
            return None

        jsonstring = response.read()
        response.close()
        try:
            data = json.loads(jsonstring)
        except (ValueError):
            return None

        # If the response doesn't look like what we expected then return None.
        if not data.has_key("response"):
            return None
        if not data["response"].has_key(expected):
            return None
        if len(data["response"][expected]) < 1:
            return None
        return data["response"][expected]
        
    def _get_sources(self, start, end):
        """ Fetches all sources that have returned data recently """
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        sources = self._get_json("", "sites")
        return Result(sources)
    
    def _get_destinations(self, src, start, end):
        """ Fetches all destinations that are available from the source """
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        destinations = self._get_json(src, "sites")
        if destinations is not None and src in destinations:
            destinations.remove(src)
        return Result(destinations)

    def _get_tests(self, src, dst, start, end):
        """ Fetches all tests that are performed between src and dst """
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        tests = self._get_json("/".join([src, dst]), "tests")
        # Just deal in test names, so create a list from the dict of {id:name}.
        if tests is not None:
            tests = tests.values()
        return Result(tests)
    
    def _get_subtypes(self, src, dst, test, start, end):
        """ Fetches all test subtypes that are performed between src and dst """
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        subtypes = self._get_json("/".join([src, dst, test]), "subtypes")
        return Result(subtypes)

    def _adjust_old_data(self, data):
        """ Strip the parent "data" that the old API uses """
        if data.has_key("data"):
            return data["data"]
        return None

    def _get_data(self, src, dst, test, subtype, start, end, binsize, rand):
        """ Fetch the data for the specified src/dst/test/timeperiod """
        # List of all data, similar format to the current REST interface.
        # TODO: Add more information about max/min/stddev etc.
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        # [ 
        #   { 
        #        "time": timestamp,
        #        "rtt_ms": { "missing": 0, "count": 1, "mean": 3 },
        #        "packetsize_bytes": { "missing": 0, "count": 1, "mean": 84 },
        #   }
        # ]

        # We may still want some random data for testing that is quick to 
        # generate (rather than waiting for real data from the old REST API).
        if rand:
            return self._get_random_data(start, end, binsize)

        args = [src, dst, test, subtype, str(start), str(end)]
        data = self._get_json("/".join(args), "dataset", binsize)
        if data is not None:
            data = map(self._adjust_old_data, data)
        return Result(data)

    def _get_random_data(self, start, end, binsize):
        """ Fetch random data for the specified src/dst/test/timeperiod """
        data = []
        now = start

        # Make up a number for how many items there are per bin.
        count = binsize / 60
        if count < 1:
            count = 1

        # Fill the whole requested time period with data.
        while now <= end:
            # Default values are for complete loss during this bin.
            rtt_mean = -1
            rtt_count = 0
            rtt_missing = count
            rtt_max = -1
            rtt_min = -1
            rtt_stddev = 0
            # If the entire bin isn't lost, calculate some random variables.
            if random.randint(1, 1000) > 5:
                # Mean is in the range 1 - 100.
                rtt_mean = random.randint(1, 100)
                # Count is in the range 1 - max items in this bin.
                if count == 1:
                    rtt_count = 1
                else:
                    rtt_count = random.randint(1, count)
                # Missing is however meany are left.
                rtt_missing = count - rtt_count
                # Make some semi-believable values for other summary statistics.
                rtt_max = rtt_mean
                rtt_min = rtt_mean
                rtt_stddev = 0
                if rtt_count > 1:
                    rtt_max += random.randint(1, 50)
                    if rtt_mean <= 2:
                        rtt_min = 1
                    else:
                        rtt_min -= random.randint(1, rtt_mean-1)
                    rtt_stddev = random.random() * rtt_mean / 2.0
                
            # Add the data point
            data.append({
                    "time": now,
                    "rtt_ms": { 
                        "missing": rtt_missing, 
                        "count": rtt_count, 
                        "mean": rtt_mean,
                        "max": rtt_max,
                        "min": rtt_min,
                        "stddev": rtt_stddev,
                    },
                    "packetsize_bytes": {
                        "missing": 0,
                        "count": count,
                        "mean": 84,
                        "max": 84,
                        "min": 84,
                        "stddev": 0,
                    },
            })
            now += binsize

        return Result(data)


class Result(object):
    """ Object to represent and facilitate fetching of results """

    def __init__(self, data):
        """ Create a new result object and associate a data source with it.
            This datasource will be queried to provide result data as needed.

            TODO: currently the datasource is simply a list, the functions to
                  fetch data etc will need to change once a database is the 
                  source
        
            Keyword arguments:
            data -- the source of data for the results
        """
        self.data = data 
        self.index = 0

    def __iter__(self):
        return self

    def next(self):
        """ Fetch the next item for the iterator """
        if self.data is None:
            raise StopIteration
        if self.index < len(self.data):
            self.index += 1
            return self.data[self.index-1]
        raise StopIteration 

    def fetchone(self):
        """ Fetch the next item in the result set, or None if all consumed """
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchall(self):
        """ Fetch all remaining items in the result set """
        if self.data is None:
            return []
        current = self.index
        self.index = len(self.data)
        return self.data[current:]

    def fetchmany(self, count):
        """ Fetch up to the specified number of results and return as a list """
        if self.data is None:
            return []
        i = 0
        result = []
        while i < count and self.index < len(self.data):
            result.append(self.data[self.index])
            self.index += 1
            i += 1
        return result

    def count(self):
        """ Return the number of results this query produced """
        if self.data is None:
            return 0
        return len(self.data)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
