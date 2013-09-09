#!/usr/bin/env python
# -*- coding: ascii -*-

import sqlalchemy

class AmpParser(object):
    """ Base class for all the AMP data types. Deals with all the things that
        the AMP data has in common - the AMP metadata database and fetching
        sources, destinations, meshes, site info, etc.
    """

    def __init__(self, dbconfig):
        """ Initialises an AMP connection """
        # These dictionaries store local copies of data that relate to
        # identifying streams, so we don't have to query the database
        # everytime someone selects a new 'source' on the graph page.
        #
        # In current amp-web, we want a set for each dropdown you implement
        # plus a 'streams' map so that you can look up the stream id once all
        # of the selection options have been set.

        # Dictionary that maps (dest) to a set of sources that test to that
        # destination. Allows us to look up sources based on a given
        # destination
        self.sources = {}

        # Dictionary that maps (source) to a set of destinations for that source
        self.destinations = {}

        # Dictionary that maps (source, dest, size) to the corresponding
        # stream id
        self.streams = {}

        # The AMP database stores site/mesh metadata
        try:
            # TODO make this configurable somewhere?
            url = sqlalchemy.engine.url.URL("postgresql", database="amp2",
                    username=dbconfig['user'], host=dbconfig['host'], 
                    password=dbconfig['pwd'])
            self.ampdb = sqlalchemy.create_engine(url)
            # test query to see if the database connection was actually made:
            # sqlalchemy is apparently stupid and doesn't let us easily check
            self.ampdb.table_names()
        except sqlalchemy.exc.OperationalError:
            self.ampdb = None

    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            The '_requesting' parameter must be set to describe which dropdown
            you are trying to populate. If not set, this function will return
            an empty list.

            If '_requesting' is set to 'sources' and 'destination' is set,
            this will return a list of sources that test to the given dest.
            If 'destination' is not set, then all known sources are returned.

            If '_requesting' is set to 'destinations' and 'source' is set,
            this will return the list of destinations tested to by that source.
            If 'source' is not set, then all possible destinations will be
            returned.
        """

        if "_requesting" not in params:
            return []

        if params["_requesting"] == 'sources':
            if 'destination' in params:
                destination = params['destination']
            else:
                destination = None
            if 'mesh' in params:
                mesh = params['mesh']
            else:
                mesh = None
            return sorted(self._get_sources(destination, mesh))

        if params["_requesting"] == "destinations":
            if 'source' in params:
                source = params["source"]
            else:
                source = None
            if 'mesh' in params:
                mesh = params['mesh']
            else:
                mesh = None
            return sorted(self._get_destinations(source, mesh))

        if params["_requesting"] == "site_info":
            if 'site' in params:
                return self._get_site_info(params['site'])
            return {}

        if params["_requesting"] == "source_meshes":
            if 'site' in params:
                return self._get_source_meshes(params['site'])
            else:
                return self._get_source_meshes(None)

        if params["_requesting"] == "destination_meshes":
            if 'site' in params:
                return self._get_destination_meshes(params['site'])
            else:
                return self._get_destination_meshes(None)
        # if we don't know what the user wants, return an empty list and
        # hopefully the more specific child class knows how to get it
        return []

    def _get_sources(self, dest, mesh):
        """ Get a list of all sources that test to a given destination.
            If the destination is None, return all known sources. Only
            sources for which there is data should be returned here.
        """
        mesh_sites = []
        # if the mesh is set then find the sites that belong
        if mesh is not None and self.ampdb is not None:
            mesh_sites = [x[0] for x in self.ampdb.execute(sqlalchemy.text(
                        "SELECT ampname FROM active_mesh_members "
                        "WHERE meshname = :mesh AND mesh_is_src = true "
                        "ORDER BY ampname"),
                    {"mesh": mesh})]

        # if dest is set, find the sources that test to it
        if dest != None:
            if dest not in self.sources:
                return []
            elif mesh is not None:
                # take the intersection of sources in mesh and sources with data
                return list(
                        set(self.sources[dest].keys()).intersection(mesh_sites))
            else:
                return self.sources[dest].keys()

        srcs = set()
        for v in self.sources.values():
            for d in v.keys():
                srcs.add(d)
        # take the intersection of sources in mesh and sources with data
        if mesh is not None:
            return list(srcs.intersection(mesh_sites))
        return list(srcs)

    def _get_destinations(self, source, mesh):
        """ Get a list of all destinations that are tested to by a given
            source. If the source is None, return all possible destinations.
        """
        # if the mesh is set then find the sites that belong
        mesh_sites = []
        if mesh is not None and self.ampdb is not None:
            mesh_sites = [x[0] for x in self.ampdb.execute(sqlalchemy.text(
                        "SELECT ampname FROM active_mesh_members "
                        "WHERE meshname = :mesh AND mesh_is_dst = true "
                        "ORDER BY ampname"),
                    {"mesh": mesh})]

        # if source is set, find the destinations it tests to
        if source != None:
            if source not in self.destinations:
                return []
            elif mesh is not None:
                return list(
                        set(self.destinations[source].keys()).intersection(
                            mesh_sites))
            else:
                return self.destinations[source].keys()

        # if mesh is set but no source, then simply return everything in the
        # mesh, regardless if there is data for it or not
        if mesh is not None:
            return mesh_sites

        dests = set()
        for v in self.destinations.values():
            for d in v.keys():
                dests.add(d)
        return list(dests)

    def _get_source_meshes(self, site=None):
        """ Fetch all source meshes, possibly filtered by a site """
        if self.ampdb is None:
            return []
        # No site set, return all possible source meshes
        if site is None:
            return [{"name":x[0], "longname":x[1], "description":x[2]}
                for x in self.ampdb.execute(
                        "SELECT mesh_name, mesh_longname, mesh_description "
                        "FROM mesh "
                        "WHERE mesh_active = true AND mesh_is_src = true")]
        # Site is set, return all source meshes that the site is a part of
        return [{"name":x[0], "longname":x[1], "description":x[2]}
            for x in self.ampdb.execute(sqlalchemy.text(
                        "SELECT mesh_name, mesh_longname, mesh_description "
                        "FROM active_mesh_members JOIN mesh "
                        "ON active_mesh_members.meshname = mesh.mesh_name "
                        "WHERE ampname = :site AND mesh_is_src = true"),
                    {"site": site})]

    def _get_destination_meshes(self, site=None):
        """ Fetch all destination meshes, possibly filtered by a site """
        if self.ampdb is None:
            return []
        # No site set, return all possible destination meshes
        if site is None:
            return [{"name":x[0], "longname":x[1], "description":x[2]}
                for x in self.ampdb.execute(
                        "SELECT mesh_name, mesh_longname, mesh_description "
                        "FROM mesh "
                        "WHERE mesh_active = true AND mesh_is_dst = true")]
        # Site is set, return all destination meshes that the site tests to
        return [{"name":x[0], "longname":x[1], "description":x[2]}
            for x in self.ampdb.execute(sqlalchemy.text(
                        "SELECT mesh_name, mesh_longname, mesh_description "
                        "FROM active_mesh_members JOIN mesh "
                        "ON active_mesh_members.meshname = mesh.mesh_name "
                        "WHERE ampname = :site AND mesh_is_dst = true"),
                    {"site": site})]

    def _get_site_info(self, site):
        """ Get more detailed and human readable information about a site """
        # if we can't find the site then return *something* they can at
        # least use, even if it doesn't have any useful information
        unknown = {
            "ampname": site,
            "longname": site,
            "description": "",
            "location": "unknown location",
        }
        if self.ampdb is None:
            return unknown
        info = self.ampdb.execute(sqlalchemy.text(
                    "SELECT site_ampname as ampname, "
                    "site_longname as longname, "
                    "site_location as location, "
                    "site_description as description, "
                    "site_active as active "
                    "FROM site WHERE site_ampname = :site"),
                    {"site": site}).first()
        if info is None:
            return unknown
        return dict(info)
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
