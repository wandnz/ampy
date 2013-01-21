#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Connects to an AMP database and interacts with it
"""

import time
import urllib2
import json
import httplib
import sys

import sqlalchemy
import ampy.result

try:
    import pylibmc
    _have_memcache = True
except ImportError:
    _have_memcache = False


class Connection(object):
    """ Class that is used to query the AMP dataset. Queries will return a
        Result object that can be iterated on
    """

    def __init__(self):
        """ Initialises an AMP connection """
        self.urlbase = "http://erg.wand.net.nz/amp/testdata2/json"
        self.apikey = "cathyisastud"

        # The database stores AMP data as well as site/mesh metadata
        url = sqlalchemy.engine.url.URL("postgresql", database="amp2")
        self.db = sqlalchemy.create_engine(url)

        # For now we will cache everything on localhost for 60 seconds.
        if _have_memcache:
            # TODO should cache duration be based on the amount of data?
            self.cache_duration = 60
            self.memcache = pylibmc.Client(
                    ["127.0.0.1"],
                    #["127.0.0.1:11212"],
                    behaviors={
                        "tcp_nodelay": True,
                        "no_block": True,
                        })
        else:
            self.memcache = False

    def get_sources(self, mesh=None, start=None, end=None):
        """ Get all source monitors """
        # TODO Filter results based on having specific test data available?
        # TODO Filter results based on data during start and end times
        if mesh is not None:
            return [x[0] for x in self.db.execute(sqlalchemy.text(
                        "SELECT ampname FROM active_mesh_members "
                        "WHERE meshname = :mesh AND mesh_is_src = true "
                        "ORDER BY ampname"),
                    {"mesh": mesh})]
        return [x[0] for x in self.db.execute(sqlalchemy.text(
                    "SELECT DISTINCT ampname FROM active_mesh_members "
                    "WHERE mesh_is_src = true ORDER BY ampname"))]

    def get_destinations(self, src=None, mesh=None, start=None, end=None):
        """ Get all destinations from the given source """
        # TODO Filter results based on having specific test data available?
        # TODO Filter results based on data during start and end times
        # src=None, mesh=set - return all dests in the given mesh
        if mesh is not None and src is None:
            return [x[0] for x in self.db.execute(sqlalchemy.text(
                        "SELECT ampname FROM active_mesh_members "
                        "WHERE meshname = :mesh AND mesh_is_dst = true "
                        "ORDER BY ampname"),
                    {"mesh": mesh})]
        # src=set, mesh=None - find all the sites that share any mesh with
        # the source (including special hidden meshes), then return all of
        # those sites that are also in any destination mesh
        # TODO this seems overly complex and verbose
        elif src is not None and mesh is None:
            return [x[0] for x in self.db.execute(sqlalchemy.text(
                        "SELECT DISTINCT ampname FROM active_mesh_members "
                        "WHERE ampname IN "
                        "(SELECT DISTINCT ampname FROM active_mesh_members "
                        "WHERE mesh_is_dst = true AND ampname != :src) "
                        "AND meshname IN ("
                        "SELECT meshname FROM active_mesh_members "
                        "WHERE ampname = :src) ORDER BY ampname"),
                    {"src" : src})]
        # src=set, mesh=set - confirm that the source is part of the
        # destination mesh and return all destinations in mesh except source
        # TODO this seems overly complex and verbose
        elif src is not None and mesh is not None:
            return [x[0] for x in self.db.execute(sqlalchemy.text(
                        "SELECT DISTINCT ampname FROM active_mesh_members "
                        "WHERE ampname != :src AND meshname IN ("
                        "SELECT DISTINCT meshname FROM active_mesh_members "
                        "WHERE ampname = :src AND meshname = :mesh "
                        "AND mesh_is_dst = true) "
                        "ORDER BY ampname"),
                    {"src": src, "mesh": mesh})]
        # If no source is given then find all possible destinations
        return [x[0] for x in self.db.execute(sqlalchemy.text(
                    "SELECT DISTINCT ampname FROM active_mesh_members "
                    "WHERE mesh_is_dst = true ORDER BY ampname"))]

    def get_tests(self, src, dst, start=None, end=None):
        """ Fetches all tests that are performed between src and dst """
        # TODO Deal with any of src or dst not being set and instead return
        # all tests to or from a host.
        return self._get_tests(src, dst, start, end)

    def get_site_info(self, site):
        """ Get more detailed and human readable information about a site """
        info = self.db.execute(sqlalchemy.text(
                    "SELECT site_ampname as ampname, "
                    "site_longname as longname, "
                    "site_location as location, "
                    "site_description as description, "
                    "site_active as active "
                    "FROM site WHERE site_ampname = :site"),
                    {"site": site}).first()
        if info is None:
            return {}
        return dict(info)

    def get_source_meshes(self, site=None):
        """ Fetch all source meshes, possibly filtered by a site """
        # No site set, return all possible source meshes
        if site is None:
            return [x[0] for x in self.db.execute(
                    "SELECT mesh_name FROM mesh "
                    "WHERE mesh_active = true AND mesh_is_src = true")]
        # Site is set, return all source meshes that the site is a part of
        return [x[0] for x in self.db.execute(sqlalchemy.text(
                    "SELECT meshname FROM active_mesh_members "
                    "WHERE ampname = :site AND mesh_is_src = true"),
                {"site": site})]
    
    def get_destination_meshes(self, site=None):
        """ Fetch all destination meshes, possibly filtered by a site """
        # No site set, return all possible destination meshes
        if site is None:
            return [x[0] for x in self.db.execute(
                    "SELECT mesh_name FROM mesh "
                    "WHERE mesh_active = true AND mesh_is_dst = true")]
        # Site is set, return all destination meshes that the site tests to
        return [x[0] for x in self.db.execute(sqlalchemy.text(
                    "SELECT meshname FROM active_mesh_members "
                    "WHERE ampname = :site AND mesh_is_dst = true"),
                {"site": site})]

    def get_recent_data(self, src, dst, test, subtype, duration, binsize=None):
        """ Fetch data for the most recent <duration> seconds and cache it """
        # Default to returning only a single aggregated response
        if binsize is None:
            binsize = duration

        # If we have memcache check if this data is available already.
        if self.memcache:
            # TODO investigate why src and dst are sometimes being given to us
            # as unicode by the tooltip data requests. Any unicode string here
            # makes the result type unicode, which memcache barfs on so for now
            # force the key to be a normal string type.
            key = str("_".join(
                    [src, dst, test, subtype, str(duration), str(binsize)]))
            try:
                if key in self.memcache:
                    #print "hit %s" % key
                    return ampy.result.Result(self.memcache.get(key))
                #else:
                #    print "miss %s" % key
            except pylibmc.SomeErrors:
                # Nothing useful we can do, carry on as if data is not present.
                pass
        
        end = int(time.time())
        start = end - duration
        args = [src, dst, test, subtype, str(start), str(end)]
        data = self._get_json("/".join(args), "dataset", binsize)
        if data is None:
            # Empty list is used as a marker, because if we use None then it
            # is indistinguishable from a cache miss when we look it up. Is
            # there a better marker we can use here?
            data = []
        else:
            data = map(self._adjust_old_data, data)

        if self.memcache:
            try:
                self.memcache.set(key, data, self.cache_duration)
            except pylibmc.WriteError:
                # Nothing useful we can do, carry on as if data was saved.
                pass
        return ampy.result.Result(data)

    def get(self, src=None, dst=None, test=None, subtype=None, start=None,
            end=None, binsize=60):
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
            # Pass through other args so we can do smart filtering?
            return self.get_sources(start=start, end=end)

        if dst is None:
            # Pass through other args so we can do smart filtering?
            return self.get_destinations(src, start, end)
        
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

        return self._get_data(src, dst, test, subtype, start, end, binsize)

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
        
    def _get_tests(self, src, dst, start, end):
        """ Fetches all tests that are performed between src and dst """
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        tests = self._get_json("/".join([src, dst]), "tests")
        # Just deal in test names, so create a list from the dict of {id:name}.
        if tests is not None:
            tests = tests.values()
        return ampy.result.Result(tests)
    
    def _get_subtypes(self, src, dst, test, start, end):
        """ Fetches all test subtypes that are performed between src and dst """
        # FIXME Temporarily fetching using existing REST API, fetch from DB.
        subtypes = self._get_json("/".join([src, dst, test]), "subtypes")
        return ampy.result.Result(subtypes)

    def _adjust_old_data(self, data):
        """ Strip the parent "data" that the old API uses """
        if data.has_key("data"):
            return data["data"]
        return None

    def _get_data(self, src, dst, test, subtype, start, end, binsize):
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
        args = [src, dst, test, subtype, str(start), str(end)]
        data = self._get_json("/".join(args), "dataset", binsize)
        if data is not None:
            data = map(self._adjust_old_data, data)
        return ampy.result.Result(data)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
