#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Connects to an AMP database and interacts with it
"""

import random
import time


class Connection(object):
    """ Class that is used to query the AMP dataset. Queries will return a
        Result object that can be iterated on
    """

    def __init__(self):
        """ Initialises an AMP connection """
        self._connect()

    def _connect(self):
        """ Connects to AMP """
        pass

    def get(self, src=None, dst=None, test=None, subtype=None, start=None, end=None, binsize=60):
        """ Fetches data from the connection, returning a Result object
        
            Keyword arguments:
            src -- source to get data for, or None to fetch all sources
            dst -- dest to get data for, or None to fetch all valid dests
            test -- test to get data for, or None to fetch all valid tests
            subtype -- subtype to get data for, or None to fetch all valid ones
            start -- timestamp for the start of the period to fetch data for
            end -- timestamp for the end of the period to fetch data for
            binsize -- number of seconds worth of data to bin
        """

        if src is None:
            # pass through other args so we can do smart filtering?
            return self._get_sources(start, end)

        if dst is None:
            # pass through other args so we can do smart filtering?
            return self._get_destinations(src, start, end)
        
        if test is None:
            # pass through other args so we can do smart filtering?
            return self._get_tests(src, dst, start, end)

        if test is None:
            # pass through other args so we can do smart filtering?
            return self._get_tests(src, dst, start, end)

        if subtype is None:
            return self._get_subtypes(src, dst, test, start, end)

        # FIXME: consider limiting maximum durations based on binsize
        # if end is not set then assume "now"
        if end is None:
            end = int(time.time())

        # if start is not set then assume 5 minutes before the end
        if start is None:
            start = end - (60*5)

        return self._get_data(src, dst, test, subtype, start, end, binsize)

    def _get_sources(self, start, end):
        """ Fetches all sources that have returned data recently """
        # FIXME temporarily hardcoded sources to test API, fetch from DB
        sources = [
            "ampz-auckland", 
            "ampz-waikato", 
            "ampz-massey-pn",
            "ampz-karen-wellington",
            "ampz-waikato:v6",
            ]
        return Result(sources)
    
    def _get_destinations(self, src, start, end):
        """ Fetches all destinations that are available from the give source """
        # FIXME temporarily hardcoded destinations to test API, fetch from DB
        destinations = [
            "ampz-auckland", 
            "ampz-waikato", 
            "ampz-massey-pn",
            "ampz-karen-wellington",
            "ampz-waikato:v6",
            "ns1.dns.net.nz",
            "www.stuff.co.nz",
            ]
        if src in destinations:
            destinations.remove(src)
        return Result(destinations)

    def _get_tests(self, src, dst, start, end):
        """ Fetches all tests that are performed between src and dst """
        # FIXME temporarily hardcoded tests to test API, fetch from DB
        tests = [ "icmp", "trace" ]
        return Result(tests)
    
    def _get_subtypes(self, src, dst, test, start, end):
        """ Fetches all test subtypes that are performed between src and dst """
        # FIXME temporarily hardcoded test subtypes to test API, fetch from DB
        subtypes = []
        if test == "icmp":
            subtypes = [ "0084", "rand" ]
        elif test == "trace":
            subtypes = [ "trace" ]
        return Result(subtypes)

    def _get_data(self, src, dst, test, subtype, start, end, binsize):
        """ Fetch the data for the specified src/dst/test/timeperiod """
        # list of all data, similar format to the REST interface 
        # TODO: add more information about max/min/stddev etc
        # FIXME temporarily hardcoded test data to test API, fetch from DB
        # [ 
        #   { 
        #        "time": timestamp,
        #        "rtt_ms": { "missing": 0, "count": 1, "mean": 3 },
        #        "packetsize_bytes": { "missing": 0, "count": 1, "mean": 84 },
        #   }
        # ]
        data = []
        now = start

        # make up a number for how many items there are per bin
        count = binsize / 60
        if count < 1:
            count = 1

        # fill the whole requested time period with data
        while now <= end:
            # default values are for loss
            rtt_mean = -1
            rtt_count = 0
            rtt_missing = count
            rtt_max = -1
            rtt_min = -1
            rtt_stddev = 0
            # if the entire bin isn't lost, calculate some random variables
            if random.randint(1, 1000) > 5:
                # mean is in the range 1 - 100
                rtt_mean = random.randint(1, 100)
                # count is in the range 1 - max items in this bin
                if count == 1:
                    rtt_count = 1
                else:
                    rtt_count = random.randint(1, count)
                # missing is however meany are left
                rtt_missing = count - rtt_count
                # make some semi-believable values for other summary statistics
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
                
            # add the data point
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
        current = self.index
        self.index = len(self.data)
        return self.data[current:]

    def fetchmany(self, count):
        """ Fetch up to the specified number of results and return as a list """
        i = 0
        result = []
        while i < count and self.index < len(self.data):
            result.append(self.data[self.index])
            self.index += 1
            i += 1
        return result

    def count(self):
        """ Return the number of results this query produced """
        return len(self.data)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
