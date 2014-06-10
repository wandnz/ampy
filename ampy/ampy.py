import operator

from libampy.ampmesh import AmpMesh
from libampy.viewmanager import ViewManager
from libampy.collection import Collection
from libampy.nntsc import NNTSCConnection
from libampy.cache import AmpyCache
from libampy.eventmanager import EventManager

from libnntscclient.logger import *

from libampy.collections.ampicmp import AmpIcmp
from libampy.collections.amptraceroute import AmpTraceroute
from libampy.collections.ampdns import AmpDns
from libampy.collections.rrdsmokeping import RRDSmokeping
from libampy.collections.rrdmuninbytes import RRDMuninbytes
from libampy.collections.lpipackets import LPIPackets
from libampy.collections.lpibytes import LPIBytes
from libampy.collections.lpiflows import LPIFlows
from libampy.collections.lpiusers import LPIUsers

class Ampy(object):
    """
    Primary class for ampy, which acts as a bridge between the Cuz website
    and the various database backends that support the site, particularly
    the NNTSC database.

    This class implements all of the external API methods required by the
    Cuz website. The website should only need to import this module to 
    access all of the ampy functionality.

    API Functions
    -------------
    start:
        Prepares Ampy for operation. Must be called prior to calling any
        other API functions.
    get_collections:
        Fetches a list of supported collections from the NNTSC database.
    get_meshes:
        Fetches a list of available AMP meshes.
    get_matrix_members:
        Fetches all the sites that are needed to construct a matrix.
    get_amp_site_info:
        Fetches detailed information about an AMP mesh member.
    get_recent_data:
        Fetches aggregated values for a time period starting at now and 
        going back a specified duration, i.e. the most recent measurements.
    get_historic_data:
        Fetches aggregated time series data for a specified time period.
    get_view_legend:
        Constructs suitable legend labels for each group present in a view. 
    get_selection_options:
        Given a set of chosen group properties, returns a set of possible
        options for the next group property.
    test_graphtab_view:
        Given an existing view for another collection, checks if there is 
        a valid equivalent view for a collection.
    create_graphtab_view:
        Given an existing view for another collection, creates an 
        equivalent view for a collection.
    get_event_view:
        Given a stream id that an event was detected on, creates a suitable
        view for displaying that event on a graph.
    modify_view:
        Adds or removes a group from an existing view.
    get_matrix_data:
        Fetches all data to populate a matrix constructed from AMP meshes.
    get_view_events:
        Fetches all events for streams belonging to a given view that 
        occurred over a certain time period.
    get_event_groups:
        Fetches all event groups for a specified time period.
    get_event_group_members:
        Fetches all events that belong to a specified event group.
    """

    def __init__(self, ampdbconf, viewconf, nntscconf, eventconf):
        """
        Init function for the Ampy class.

        Parameters:
          ampdbconf -- config for connecting to the AMP mesh database.
          viewconf -- config for connecting to the Views database.
          nntscconf -- config for connecting to the NNTSC exporter.
          eventconf -- config for connecting to the Events database.

        Refer to the AmpyDatabase class for more details on the 
        configuration required for connecting to databases via ampy.

        Refer to the NNTSCConnection class for more details on the 
        configuration required for connecting to a NNTSC exporter.
        """
        self.ampmesh = AmpMesh(ampdbconf)
        self.viewmanager = ViewManager(viewconf)
        self.nntscconfig = nntscconf
        self.cache = AmpyCache(12)
        self.eventmanager = EventManager(eventconf)

        self.collections = {}
        self.savedcoldata = {}
        self.started = False;

    def start(self):
        """
        Ensures ampy is ready for subsequent API calls by populating the 
        internal list of available collections.

        Must be called once the Ampy instance is instantiated, i.e. before
        making any other API calls. start() only needs to be called once 
        per Ampy instance.

        Returns:
          None if the collection query fails, otherwise returns the number
          of supported collections.
        """
        # Only fetch the collections the first time this is called.
        if self.started:
            return len(self.savedcoldata)

        retval = self._query_collections()
        if retval is None:
            return None
        self.started = True
        return retval

    def get_collections(self):
        """
        Fetches a list of collections available via this Ampy instance.

        Returns:
          a list of collection names
        """
        return self.savedcoldata.keys()

    def get_meshes(self, endpoint):
        """
        Fetches all source or destination meshes.

        Parameters:
          endpoint -- either "source" or "destination", depending on 
                      which meshes are required.

        Returns:
          a list of dictionaries that describe the available meshes or 
          None if an error occurs while querying for the meshes.

        Mesh dictionary format:
          The returned dictionaries should contain three elements:
            name -- the internal unique identifier string for the mesh
            longname -- a string containing a mesh name that is more 
                        suited for public display
            description -- a string describing the purpose of the mesh in
                           reasonable detail
        """
        return self.ampmesh.get_meshes(endpoint)

    def get_matrix_members(self, sourcemesh, destmesh):
        """
        Fetches all the sites that are needed to construct a matrix.

        Parameters:
          sourcemesh -- the mesh that represents the test sources
          destmesh -- the mesh that represents the test targets

        Returns:
          a tuple containing two lists: the first is the list of sources,
          the second is the list of targets.
          Returns None if the query fails.
        """
        sources = self.ampmesh.get_sources(sourcemesh)
        if sources is None:
            return None

        dests = self.ampmesh.get_destinations(destmesh)
        if dests is None:
            return None

        return (sources, dests)

    def get_amp_site_info(self, sitename):
        """
        Fetches details about a particular AMP mesh member.

        Parameters:
          sitename -- the name of the mesh member to query for

        Returns: 
          a dictionary containing detailed information about the site.

        The resulting dictionary contains the following items:
          ampname -- a string containing the internal name for the site
          longname -- a string containing a site name that is suitable for
                      public display
          location -- a string containing the city or data-centre where the
                      amplet is located (if there is one for that site)
          description -- a string containing any additional descriptive
                         information about the site
          active -- a boolean flag indicating whether the site is currently
                    active
        """
        return self.ampmesh.get_site_info(sitename)
        

    def get_recent_data(self, collection, view_id, duration, detail):
        """
        Fetches summary statistics for each label within a view that
        summarise the most recent measurements collected for each label.

        The resulting data will be aggregated into a single value for the
        entire time period, making this function best suited for summary
        statistics (e.g. matrix tooltips) rather than drawing time series 
        graphs.

        See get_historic_data if you need time series data.

        Parameters:
          collection -- the name of the collection that the view belongs to.
          view_id -- the view to fetch recent data for.
          duration -- the length of the time period to fetch data for, in
                      seconds. 
          detail --  the level of detail, e.g. 'full', 'matrix'. This will
                     determine which data columns are queried and how they
                     are aggregated.

        Returns:
          a tuple containing two elements. The first is a dictionary
          mapping label identifier strings to a list containing the
          summary statistics for that label. The second is a list of
          labels which failed to fetch recent data due to a database query
          timeout.

        """
        alllabels = []

        # Most of the work here is finding all of the labels for the
        # view we're given.
        col, viewgroups = self._view_to_groups(collection, view_id)
        if col is None:
            log("Failed to fetch recent data")
            return None

        for gid, descr in viewgroups.iteritems():
            grouplabels = col.group_to_labels(gid, descr, True)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue
            alllabels += grouplabels

        return self._fetch_recent(col, alllabels, duration, detail)


    def get_historic_data(self, collection, view_id, start, end, 
            detail, binsize = None):
        """
        Fetches aggregated time series data for each label within a view.

        Parameters:
          collection -- the name of the collection that the view belongs to.
          view_id -- the view to fetch time series data for.
          start -- a timestamp indicating when the time series should begin.
          end -- a timestamp indicating when the time series should end.
          detail --  the level of detail, e.g. 'full', 'matrix'. This will
                     determine which data columns are queried and how they
                     are aggregated.
          binsize -- the desired aggregation frequency. If None, this will
                     be automatically calculated based on the time period
                     that you asked for.

        Returns:
          a dictionary keyed by label where each value is a list containing
          the aggregated time series data for the specified time period. 
          Returns None if an error occurs while fetching the data.

        """
        alllabels = []

        col, viewgroups = self._view_to_groups(collection, view_id)
        if col is None:
            log("Failed to fetch historic data")
            return None

        if binsize is None:
            binsize = col.calculate_binsize(start, end, detail)

        # Break the time period down into blocks for caching purposes
        extra = col.extra_blocks(detail)
        blocks = self.cache.get_caching_blocks(start, end, binsize, extra)

        # Find all labels for this view and their corresponding streams
        for gid, descr in viewgroups.iteritems():
            grouplabels = col.group_to_labels(gid, descr, True)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue
            alllabels += grouplabels

        # Figure out which blocks are cached and which need to be queried 
        notcached, cached = self._find_cached_data(col, blocks, alllabels, 
                binsize, detail)

        # Fetch all uncached data
        fetched = frequencies = timeouts = {}
        if len(notcached) != 0:
            fetch = self._fetch_uncached_data(col, notcached, binsize, detail)
            if fetch is None:
                return None

            fetched, frequencies, timeouts = fetch

        # Merge fetched data with cached data to produce complete series

        data = {}
        for label, dbdata in fetched.iteritems():
            data[label] = []
            failed = timeouts[label]

            for b in blocks:
                blockdata, dbdata = self._next_block(col, b, cached[label], 
                    dbdata, frequencies[label], binsize)
                
                data[label] += blockdata

                # Store this block in our cache for fast lookup next time
                # If it already is there, we'll reset the cache timeout instead
                failed = self.cache.store_block(b, blockdata, label, binsize, 
                        detail, failed)

        
        # Any labels that were fully cached won't be touched by the previous
        # bit of code so we need to check the cached dictionary for any
        # labels that don't appear in the fetched data and process those too

        for label, item in cached.iteritems():
            
            # If the label is present in our returned data, we've already
            # processed it
            if label in data:
                continue
            data[label] = []
            
            # Slightly repetitive code but seems silly to create a 10 parameter
            # function to run these few lines of code
            for b in blocks:
                blockdata, ignored = self._next_block(col, b, cached[label], 
                        [], 0, binsize)
                data[label] += blockdata
                ignored = self.cache.store_block(b, blockdata, label, binsize, detail, [])


        return data

    def get_view_legend(self, collection, view_id):
        """
        Generates appropriate legend label strings for each group in a given
        view.

        Parameters:
          collection -- the name of the collection that the view belongs to.
          view_id -- the view to generate legend labels for.
        
        Returns:
          a list of dictionaries, where each dictionary describes a group.
          Returns None if an error occurs.
          
        Group dictionary format:
          A group dictionary contains three elements:
            group_id -- a unique string identifying the group 
            label -- the text to display on the legend for the group
            lines -- a list of 'lines' that will be drawn on the graph for 
                     the group

          A line is a tuple, containing the following three elements:
            * a unique string identifying the line (includes the group id)
            * text to display if the line is moused over on the graph or 
              legend
            * a unique integer index that indicates which colour should be 
              used to draw the line on a graph. Indexes start at zero for 
              the first line and increment from there.

        """
        col, viewgroups = self._view_to_groups(collection, view_id)
        if col is None:
            log("Failed to generate legend")
            return None

        legend = []

        # What we need:
        # A set of 'legend' entries, one per group
        # For each entry, we also need a set of 'lines', one per group member

        nextlineid = 0
        
        # Sort the groups in the view by description.
        # This ensures that A) the legend will be in a consistent order
        # and B) the ordering is more obvious to the user (i.e. alphabetical
        # starting with the first group property)
        viewgroups = sorted(viewgroups.iteritems(), key=operator.itemgetter(1))
        for gid, descr in viewgroups:
            legendtext = col.get_legend_label(descr)
            if legendtext is None:
                legendtext = "Unknown"

            # Don't lookup the streams themselves if we can avoid it
            grouplabels = col.group_to_labels(gid, descr, False)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue
            lines = []

            # Yes, we could assign line ids within group_to_labels but
            # then anyone implementing a collection has to make sure they
            # remember to do it. Also these ids are only needed for legends,
            # but group_to_labels is also used for other purposes so it
            # is cleaner to do it here even if it means an extra iteration 
            # over the grouplabels list.
            for gl in grouplabels:
                lines.append((gl['labelstring'], gl['shortlabel'], nextlineid))
                nextlineid += 1

            legend.append({'group_id':gid, 'label':legendtext, 'lines':lines})

        return legend
        

    def get_selection_options(self, collection, selected):
        """
        Given a set of known stream properties, finds the next unknown 
        stream property where there is more than one choice available 
        and returns a list of available options for that property. Any 
        stream properties along the way that have only one possible choice 
        are also returned.

        This is primarily used to populate dropdown lists on the modal
        dialogs for selecting what series to display on a graph.

        Parameters:
          collection -- the name of the collection that the view belongs to.
          selected -- a list containing the stream properties that 
                      have already been selected, e.g. the choices already 
                      made on the modal dialog, in order.

        Returns:
          a dictionary where the key is a stream property and the value is
          a list of possible choices for that stream property, given the
          properties already chosen in the 'selected' dictionary.
          
          Returns None if an error occurs while fetching the selection 
          options.
        """

        col = self._getcol(collection)
        if col == None:
            log("Error while fetching selection options")
            return None

        # Make sure we have an up-to-date stream hierarchy 
        if col.update_streams() is None:
            log("Error while fetching selection options")
            return None

        seldict = col.create_properties_from_list(selected, 
                col.streamproperties)
        if seldict is None:
            log("Unable to understand selected options")
            return None

        # The collection module does most of the work here
        options = col.get_selections(seldict)
        return options       

    

    def test_graphtab_view(self, collection, tabcollection, view_id):
        """
        Checks whether it would be possible to generate a valid view that
        is equivalent to a view from another related collection.

        Used to determine which 'related' collections should be included
        in the quick-switch tabs on the right of a graph.

        Essentially, this function attempts to convert the group 
        descriptions from the original view into group descriptions for 
        the current collection. 
        
        If any view group is successfully translated to the new collection, 
        this is considered a success even if other groups cannot be 
        translated.
        
        Parameters:
          collection -- the name of the collection that the original view
                        belongs to.
          tabcollection -- the name of the collection that the view is to 
                           be translated to.
          view_id -- the ID number of the view that is to be translated.

        Returns:
          True if the view can be translated to the new collection. 
          False if the view cannot be translated. 
          None if an error occurs while evaluating the translation.

        Note:
          If collection and tabcollection are the same, this function 
          should ALWAYS return True.
        """
        
        col, groups = self._view_to_groups(collection, view_id)   
        if col == None:
            log("Error while constructing tabview")
            return None
        
        # Make sure our target collection is also up-to-date
        tabcol = self._getcol(tabcollection)
        if tabcol == None:
            log("Error while constructing tabview")
            return None
        if tabcol.update_streams() is None:
            log("Error while fetching selection options for tab collection")
            return None

        # Translate each group in turn
        for gid, descr in groups.iteritems():
            grouprule = col.parse_group_description(descr)

            tabrule = tabcol.translate_group(grouprule)
            if tabrule is None:
                continue

            labels = tabcol.group_to_labels('tabcheck', tabrule, True)
            for lab in labels:
                # We can bail as soon as we get one group with a stream
                if len(lab['streams']) > 0:
                    return True
        
        # If we get here, none of the translated groups would match any
        # streams in the database
        return False

    def create_graphtab_view(self, collection, tabcollection, view_id):
        """
        Creates a new view for a collection based on an existing view
        for another collection.

        This is used to construct the view to be displayed if a user
        clicks on a 'related' graph tab while looking at a graph.

        Essentially, this function converts the group descriptions from the 
        original view into group descriptions for the current collection. 
        If a group cannot be converted, it will not be included on the
        new graph.
        
        Parameters:
          collection -- the name of the collection that the original view
                        belongs to.
          tabcollection -- the name of the collection that the view is to 
                           be translated to.
          view_id -- the ID number of the view that is to be translated.

        Returns:
            The ID number of the new view.
            Returns None if an error occurs while creating the new view or
            if none of the groups from the original view can be represented
            in the specified new collection.

        Note:
          If collection and tabcollection are the same, this function should
          ALWAYS return the view_id that was passed in.
        """
        col, groups = self._view_to_groups(collection, view_id)   
        if col == None:
            log("Error while constructing tabview")
            return None
        
        # Make sure our target collection is also up-to-date
        tabcol = self._getcol(tabcollection)
        if tabcol == None:
            log("Error while constructing tabview")
            return None
        if tabcol.update_streams() is None:
            log("Error while fetching selection options for tab collection")
            return None

        # Translate each group in turn
        tabgroups = []
        for gid, descr in groups.iteritems():
            grouprule = col.parse_group_description(descr)

            tabrule = tabcol.translate_group(grouprule)
            if tabrule is None:
                continue

            tabid = self.viewmanager.get_group_id(tabcollection, tabrule)
            if tabid is None:
                continue

            tabgroups.append(tabid)

        # If no groups were successfully translated to the new collection,
        # bail as we have nothing to draw on the graph.
        # Normally, this wouldn't happen as someone should have called
        # test_graphtab_view before drawing the tab that would trigger this
        # function call but you can never predict how people will abuse 
        # this API in the future
        if len(tabgroups) == 0:
            log("Unable to create tabview %s to %s for view %s" % \
                    (collection, tabcollection, view_id))
            log("No valid groups were found for new tab")
            return None

        # View groups should always be in sorted order
        tabgroups.sort()

        # Create ourselves a new view
        tabview = self.viewmanager.get_view_id(tabcollection, tabgroups)
        if tabview is None:
            log("Unable to create tabview %s to %s for view %s" % \
                    (collection, tabcollection, view_id))
            log("Could not create new view")
            return None

        return tabview

    def get_event_view(self, collection, stream):
        """
        Given a stream that an event was detected on by netevmon, generates
        a suitable view for showing the event on a graph.

        This is used to create links to graphs from events shown on the
        dashboard. Events have no concept of views or view groups; instead
        we only have the ID of *one* of the streams that contributed to the 
        time series that the event was detected on. Therefore, we need to 
        be able to convert the single stream into a group that covers all
        of the streams that contributed to the event time series.

        Parameters:
          collection -- the name of the collection that the stream 
                        belongs to.
          stream -- the ID of the stream that the event was detected on.

        Returns:
          the ID of the view generated for that stream

        """

        # Check if we have a cache entry for this stream
        cachedview = self.cache.search_stream_view(stream)
        if cachedview is not None:
            # Reset the cache timeout
            self.cache.store_stream_view(stream, cachedview)
            return cachedview

        col = self._getcol(collection)
        if col == None:
            log("Error while creating event view")
            return None
        
        if col.update_streams() is None:
            log("Error while creating event view")
            return None
        
        # Find the stream in our stream hierarchy
        streamprops = col.find_stream(stream)
        if streamprops is None:
            log("Error while creating event view")
            log("Stream %s does not exist for collection %s" % \
                    (stream, collection))
            return None

        # Convert the stream properties into a group description that we can
        # use to create a view. Note that the group may include other streams
        # in addition to the one we are looking for -- this is intentional.
        eventgroup = col.create_group_description(streamprops)
        if eventgroup is None:
            log("Error while creating event view")
            log("Unable to generate group for stream id %s (%s)" % \
                    (stream, collection))
            return None

        view = self.viewmanager.add_groups_to_view(collection, 0, [eventgroup])

        # Put the view in the cache for future lookups
        self.cache.store_stream_view(stream, view)
        return view

    def modify_view(self, collection, view_id, action, options):
        """
        Adds or removes a group from an existing view.

        Parameters:
          collection -- the name of the collection that the view belongs to.
          view_id -- the ID number of the view to be modified
          action -- either "add" if adding a group or "del" if removing one.
          options -- if adding a group, this is an ordered list of group
                     properties describing the group to add. If removing a
                     group, this is a list containing the ID of the group to
                     be removed.

        Returns:
          the ID of the resulting modified view. 
          If the view is unchanged, the original view ID will be returned.
          None if an error occurs while creating the new view.

        Note that the options parameter is always a list. In the case of
        adding a group, the options list should be a complete list of group
        properties in the order that they appear in the collection's group
        properties list. For example, adding an amp-icmp group would require
        options to be a list containing a source, a destination, a packet 
        size and an address family (in that order). 

        In the case of removing a group, the list should contain only one
        item: the group ID of the group to be removed. 
          
        """
          
        col = self._getcol(collection)
        if col == None:
            return None
        if len(options) == 0:
            return view_id


        if action == "add":
            newgroup = col.create_group_from_list(options)
            if newgroup is None:
                return view_id
            return self.viewmanager.add_groups_to_view(collection, view_id, 
                    [newgroup])
        elif action == "del":
            # XXX In theory, we could support removing more than one group?
            groupid = int(options[0])
            return self.viewmanager.remove_group_from_view(collection, 
                    view_id, groupid)
        else:
            return view_id

    def get_matrix_data(self, collection, options, duration):
        """
        Fetches all of the data required to populate an AMP matrix.

        Parameters:
          collection -- the name of the collection that the matrix 
                        belongs to.
          options -- an ordered list describing which meshes should appear
                     in the matrix.
          duration -- the amount of recent data that should be queried for
                      each matrix cell, in seconds.

        Returns:
          a tuple containing five elements. The first is a dictionary
          mapping label identifier strings to a list containing the
          summary statistics for that label. The second is a list of
          labels which failed to fetch recent data due to a database query
          timeout. The third is a list of source mesh members. The fourth
          is a list of destination mesh members. The fifth is a dictionary
          containing the view ids for the graphs that each matrix cell
          will link to.

          Returns None if an error occurs while determining the matrix 
          groups or querying for the matrix data.

        The options parameter must contain at least two items: the source 
        and destination meshes for the matrix. Any subsequent items are 
        ignored.
        
        """

        col = self._getcol(collection)
        if col == None:
            log("Error while fetching matrix data")
            return None
        
        # Make sure we have an up-to-date set of streams
        if col.update_streams() is None:
            return None
        
        # Work out which groups are required for this matrix 
        matrixgroups = self._get_matrix_groups(col, options)
        if matrixgroups is None:
            return None

        groups, sources, destinations, views = matrixgroups

        fetcheddata = self._fetch_recent(col, groups, duration, "matrix")
        if fetcheddata is None:
            return None

        return fetcheddata[0], fetcheddata[1], sources, destinations, views

    def get_view_events(self, collection, view_id, start, end):
        """
        Finds all events that need to be displayed for a given graph.

        Parameters:
          collection -- the name of the collection that the view belongs to.
          view_id -- the ID of the view that is being shown on the graph.
          start -- the timestamp at the start of the time period shown on 
                   the graph.
          end -- the timestamp at the end of the time period shown on the 
                 graph.

        Returns:
          A list of events that were detected between 'start' and 'end' 
          for all streams that are part of the view being displayed.
          Returns None if an error occurs while fetching the events.
        """

        col, groups = self._view_to_groups(collection, view_id)   
        if col == None:
            log("Error while fetching events for a view")
            return None
      
        # Convert our view groups into a set of stream labels. In particular,
        # we will need the list of streams for each label as the events are
        # associated with stream IDs, not labels or groups or views.
        alllabels = [] 
        for gid, descr in groups.iteritems():
            grouplabels = col.group_to_labels(gid, descr, True)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue

            alllabels += grouplabels

        return self.eventmanager.fetch_events(alllabels, start, end)

    def get_event_groups(self, start, end):
        """
        Finds all of the event groups that occured within a given time 
        period.

        Parameters:
          start -- the timestamp at the start of the time period of 
                   interest.
          end -- the timestamp at the end of the time period of interest.

        Returns:
          a list of event groups or None if an error occurs while querying 
          the event database.
        """
        return self.eventmanager.fetch_groups(start, end)

    def get_event_group_members(self, eventgroupid):
        """
        Fetches the events that belong to a specific event group.

        Used to populate the event list when a user clicks on an event
        group on the dashboard.

        Parameters:
          groupid -- the unique id of the event group

        Returns:
          a list of events or None if there was an error while querying 
          the event database.
        """
        return self.eventmanager.fetch_event_group_members(eventgroupid)


    def _fetch_recent(self, col, alllabels, duration, detail):
        """
        Internal function for querying NNTSC for 'recent' data, i.e. summary
        statistics for a recent time period leading up until the current time.

        If there is recent data for a queried label in the cache, that will be
        used. Otherwise, a query will be made to the NNTSC database.

        Parameters:
          col -- the module for the collection that the recent data belongs to.
          alllabels -- a list of labels that require recent data.
          duration -- the amount of recent data that should be queried for, in
                      seconds.
          detail --  the level of detail, e.g. 'full', 'matrix'. This will
                     determine which data columns are queried and how they
                     are aggregated.
        
        Returns:
            a tuple containing two elements. The first is a dictionary
            mapping label identifier strings to a list containing the
            summary statistics for that label. The second is a list of
            labels which failed to fetch recent data due to a database query
            timeout.
            Returns None if an error is encountered while fetching the data.
        
        A label in the alllabels dictionary must be a dictionary containing
        at least two elements: 
            labelstring -- an unique string identifying the label
            streams -- a list of stream IDs that belong to the label.

        """
        recent = {}
        timeouts = {}
        uncached = {}
        querylabels = {}
        end = int(time.time())
        start = end - duration
        
       
        for lab in alllabels:
            # Check if we have recent data cached for this label
            # Attach the collection to the cache label to avoid matching
            # cache keys for both latency and hop count matrix cells
            cachelabel = lab['labelstring'] + "_" + col.collection_name
            if len(cachelabel) > 128:
                log("Warning: matrix cache label %s is too long for memcache" % (cachelabel))

            cachehit = self.cache.search_recent(cachelabel, duration, detail)
            # Got cached data, add it directly to our result
            if cachehit is not None:
                recent[lab['labelstring']] = cachehit
                continue

            # Not cached, need to fetch it
            # Limit our query to active streams
            lab['streams'] = col.filter_active_streams(lab['streams'], 
                    start, end)
   
            # If no streams were active, don't query for them. Instead
            # add an empty list to the result for this label.
            if len(lab['streams']) == 0:
                recent[lab['labelstring']] = []
            else:
                querylabels[lab['labelstring']] = lab['streams']

        if len(querylabels) > 0:
            # Fetch data for labels that weren't cached using one big
            # query
            result = col.fetch_history(querylabels, start, end, duration, 
                    detail)

            for label, queryresult in result.iteritems():
                formatted = col.format_list_data(queryresult['data'], queryresult['freq']) 
                # Cache the result
                cachelabel = label + "_" + col.collection_name
                if len(cachelabel) > 128:
                    log("Warning: matrix cache label %s is too long for memcache" % (cachelabel))
                self.cache.store_recent(cachelabel, duration, detail, formatted)

                # Add the result to our return dictionary
                recent[label] = formatted
                
                # Also update the timeouts dictionary
                if len(queryresult['timedout']) != 0:
                    timeouts.append(label)

        return recent, timeouts

    def _next_block(self, col, block, cached, queried, freq, binsize):
        """
        Internal function for populating a time series block with the correct
        datapoints from a NNTSC query result for a particular label.

        This function will also insert 'gaps' into the block if a measurement
        is missing from the query result.

        In theory this should be straightforward but gets very complicated
        due to a couple of factors:
         1. the measurement frequency may be larger than the requested binsize
            (which we use for sizing our blocks), so no aggregation has
            occured. 
         2. measurements are not guaranteed to happen at exactly the timestamp
            you expect; they may be delayed by several seconds, especially
            some AMP tests.

        These factors mean that in certain cases it can be very difficult
        to tell whether a measurement is missing or whether it is just late.
        As long as the database is aggregating data for us, this isn't a
        problem -- everything gets aligned to the correct bin.

        In short, don't touch this code unless you really know what you are
        doing and are prepared to work through all the edge cases to make
        sure you don't break anything.

        Parameters:
          col -- the module for the collection that the recent data belongs to.
          block -- a dictionary describing the boundaries of the block that
                   is to be populated.
          cached -- a dictionary containing cached blocks for this label.
          queried -- a list of data points for this label fetched from NNTSC.
          freq -- the measurement frequency for this label as reported by 
                  NNTSC.
          binsize -- the requested aggregation frequency when querying NNTSC.

        Returns:
          a tuple with two elements. The first is a list of data points that
          belong to the specified block, including empty datapoints for any
          gaps in the measurement data. The second is an updated 'queried'
          list where the data points that were assigned to the block have
          been removed.

        """

        # If this block is cached, we can return the cached data right away
        if block['start'] in cached:
            return cached[block['start']], queried
       
        if freq > binsize:
            # Measurements do not align nicely with our request binsize so
            # be very careful about how we match query results to blocks
            incrementby = freq
            usekey = 'timestamp'

            # Measurements don't always happen exactly on the frequency,
            # i.e. there can be a second or two of delay. Ideally, we
            # should account for this when we are searching for the next
            # data point
            delayfactor = 10 
            
        else:
            incrementby = binsize
            usekey = 'binstart'

            # The database will always give us nice round timestamps
            # based on the requested binsize
            delayfactor = 0
   
        blockdata = []
        ts = block['start']

        while ts < block['end']:
            # We are unlikely to be predicting the future
            if ts > time.time():
                break

            if len(queried) == 0:
                # No more queried data so we must be missing a measurement
                if block['end'] - ts >= incrementby:
                    datum = {"binstart":ts, "timestamp":ts}
                ts += incrementby
            else:
                nextdata = int(queried[0][usekey])
                maxts = ts + incrementby
                if maxts > block['end']:
                    maxts = block['end']

                # We should only have one measurement per timestamp, but
                # it is unfortunately possible for us to end up with more
                # than one under very certain cases (usually freq > binsize
                # and measurements getting severely delayed, e.g. 
                # prophet->afrinic ipv6)
                #
                # Trying to aggregate the multiple measurements is difficult
                # and not really something ampy should be doing so I'm
                # just going to discard any additional measurements after
                # the first one
                if nextdata < ts:
                    ts = nextdata + incrementby
                    queried = queried[1:]
                    continue
                
                if nextdata < maxts:
                    # The next available queried data point fits in the 
                    # bin we were expecting, so format it nicely and
                    # add it to our block
                    datum = col.format_single_data(queried[0], freq)
                    queried = queried[1:]
                    if freq > binsize:
                        datum['binstart'] = ts
                    ts += incrementby

                elif ts + incrementby <= nextdata:
                    # Next measurement doesn't appear to match our expected
                    # bin, so insert a 'gap' and move on
                    datum = {"binstart":ts, "timestamp":ts}
                    ts += incrementby
                else:
                    ts += incrementby
                    continue
            
            # Always add the first datum, even if it is a missing measurement.
            # This ensures that our graphing code will force a gap between the
            # previous block and this one if the first measurement is missing.
            blockdata.append(datum)

        return blockdata, queried


    
    def _find_cached_data(self, col, blocks, labels, binsize, detail):
        """
        Determines which data blocks for a set of labels are cached and 
        which blocks need to be queried.

        Parameters:
          col -- the module for the collection that the labels belong to.
          blocks -- a list of dictionaries describing the blocks for which
                    data is required.
          labels -- a list of labels that data is required for.
          binsize -- the aggregation frequency required for the data.
          detail -- the level of detail required for the data.

        Returns:
          a tuple containing two dictionaries. The first dictionary describes
          the time periods where required data was not present in the cache 
          and therefore must be queried. The second dictionary describes 
          the blocks that were cached, including the cached data.

        """
        notcached = {}
        cached = {}

        if len(blocks) == 0:
            return notcached, cached

        start = blocks[0]['start']
        end = blocks[-1]['end']

        for label in labels:
            # Check which blocks are cached and which are not
            missing, found = self.cache.search_cached_blocks(blocks, 
                    binsize, detail, label['labelstring'])
            
            cached[label['labelstring']] = found
            
            # This skips the active stream filtering if the entire label is
            # already cached
            if len(missing) == 0:
                continue
            
            # Remove inactive streams from the streams list
            label['streams'] = col.filter_active_streams(label['streams'], 
                    start, end)

            # Add missing blocks to the list of data to be fetched from NNTSC
            for b in missing:        
                if b not in notcached:
                    notcached[b] = {label['labelstring']: label['streams']}
                else:
                    notcached[b][label['labelstring']] = label['streams']

        return notcached, cached

    def _fetch_uncached_data(self, col, notcached, binsize, detail):
        """
        Queries NNTSC for time series data that was not present in the cache.

        Parameters:
          col -- the module for the collection that the time series belongs 
                 to.
          notcached -- a dictionary describing time periods that need to
                       be queried and the labels that need to be queried
                       for those times.
          binsize -- the aggregation frequency to use when querying.
          detail -- a string that is used to determine which columns to
                    query and how to aggregate them.

        Returns:
          a tuple containing three items.
          The first item is a dictionary containing the time series data
          received for each label queried. Each time series is stored in a
          list.
          The second item is a dictionary containing the estimated 
          measurement frequency for each label queried.
          The third item is a dictionary containing a list of tuples that
          describe any time periods where the database query failed due
          to a timeout.

          Will return None if an error occurs while querying the database.
        """

        fetched = {}
        frequencies = {}
        timeouts = {}

        # Query NNTSC for all of the missing blocks
        for (bstart, bend), labels in notcached.iteritems():
            hist = col.fetch_history(labels, bstart, bend-1, binsize, detail)
            if hist is None:
                log("Error fetching historical data from NNTSC")
                return None
            
            for label, result in hist.iteritems():
                if label not in fetched:
                    fetched[label] = []
                    timeouts[label] = []
                fetched[label] += result['data']
                frequencies[label] = result['freq']
                timeouts[label] += result['timedout']
        
        return fetched, frequencies, timeouts

    
    def _query_collections(self):
        """
        Fetches the set of available collections from NNTSC and updates
        the internal collection dictionary accordingly.

        Returns:
          the number of collections fetched or None if an error occurs
          while querying the database.
        """
        nntscdb = NNTSCConnection(self.nntscconfig)

        collections = nntscdb.request_collections()
        if collections is None:
            log("Failed to query NNTSC for existing collections")
            return None

        for col in collections:
            name = col['module'] + "-" + col['modsubtype']
            self.savedcoldata[name] = col['id']

        return len(self.savedcoldata.keys())

    def _getcol(self, collection):
        """
        Finds the collection module that matches the provided collection
        name. If this Ampy instance does not have an instance of that
        collection module, one is created.

        When adding new collection modules, this function needs to be 
        updated to ensure that Ampy will be able to utilise the new module.

        Parameters:
          collection -- the name of the collection required.

        Returns:
          an instance of the collection module matching the given name, or
          None if the name does not match any known collections.

        """
        newcol = None
        # If we have a matching collection, return that otherwise create a
        # new instance of the collection

        if collection in self.collections:
            return self.collections[collection]

        if collection not in self.savedcoldata:
            log("Collection type %s does not exist in NNTSC database" % \
                    (collection))
            return None
   
        colid = self.savedcoldata[collection] 
        if collection == "amp-icmp":
            newcol = AmpIcmp(colid, self.viewmanager, self.nntscconfig)
        if collection == "amp-traceroute":
            newcol = AmpTraceroute(colid, self.viewmanager, self.nntscconfig)
        if collection == "amp-dns":
            newcol = AmpDns(colid, self.viewmanager, self.nntscconfig)
        if collection == "rrd-smokeping":
            newcol = RRDSmokeping(colid, self.viewmanager, self.nntscconfig)
        if collection == "rrd-muninbytes":
            newcol = RRDMuninbytes(colid, self.viewmanager, self.nntscconfig)
        if collection == "lpi-packets":
            newcol = LPIPackets(colid, self.viewmanager, self.nntscconfig)
        if collection == "lpi-bytes":
            newcol = LPIBytes(colid, self.viewmanager, self.nntscconfig)
        if collection == "lpi-flows":
            newcol = LPIFlows(colid, self.viewmanager, self.nntscconfig)
        if collection == "lpi-users":
            newcol = LPIUsers(colid, self.viewmanager, self.nntscconfig)

        if newcol is None:
            log("Unknown collection type: %s" % (collection))
            return None

        self.collections[collection] = newcol
        return newcol

    def _view_to_groups(self, collection, view_id):
        """
        Internal utility function that finds the collection module and
        set of view groups for a given view. Also updates the set of
        known streams for the collection.

        Used as a first step by many of the API functions.

        Parameters:
          collection -- a string with the name of the collection that the
                        view belongs to.
          view_id -- the ID number of the view.

        Returns:
          a tuple containing two items.
          The first item is the collection module.
          The second item is a dictionary of groups for the view, keyed by
          group ID. The values are strings describing each group.
        
          Returns (None, None) if any of the steps undertaken during this
          function fails.
        """

        # Find the collection module for the view
        col = self._getcol(collection)
        if col == None:
            return None, None
        
        # Make sure we have an up-to-date set of streams
        if col.update_streams() is None:
            return None, None
        
        # Check if the groups are in the cache
        cachedgroups = self.cache.search_view_groups(view_id)
        if cachedgroups is not None:
            # Refresh the cache timeout
            self.cache.store_view_groups(view_id, cachedgroups)
            return col, cachedgroups

        # Otherwise, we'll have to query the views database
        viewgroups = self.viewmanager.get_view_groups(collection, view_id)

        if viewgroups is None:
            log("Unable to find groups for view id %d(%s)" % \
                    (view_id, collection))
            return None, None
       
        # Put these groups in the cache
        self.cache.store_view_groups(view_id, viewgroups)
        
        return col, viewgroups

    def _get_matrix_groups(self, col, options):
        """
        Internal function for finding the groups necessary to populate
        a matrix.

        Parameters:
          col -- the module for the collection being shown on the matrix.
          options -- a list of parameters describing the matrix properties.

        Returns:
          a tuple containing three items:
            1. a list of dictionaries describing each of the groups that 
               should be present in the matrix. 
            2. a list of sites belonging to the source mesh.
            3. a list of sites belonging to the destination mesh.

          Returns None if an error occurs while finding matrix groups.

        The options parameter must contain at least two items: the source and
        destination meshes for the matrix. Any subsequent items are used
        by the collection module to determine the groups that correspond to
        each matrix cell. Using amp-icmp as an example, the third item in
        the options list should be a packet size. If provided, this would
        limit the matrix cells to only include streams that matched the
        given packet size.
       
        The returned group dictionaries contain at least two items:
          labelstring -- a unique string identifying that group
          streams -- a list of stream IDs that belong to the group
        
        """
         
        if len(options) < 2:
            log("Invalid options for fetching matrix streams")
            return None

        # Make sure we have an up-to-date list of streams before looking
        # up any groups
        if col.update_streams() is None:
            log("Error while fetching matrix streams")
            return None
        
        # First two options must be the source and destination meshes
        sourcemesh = options[0]
        destmesh = options[1]

        sources = self.ampmesh.get_sources(sourcemesh)
        if sources is None:
            log("Error while fetching matrix streams")
            return None

        destinations = self.ampmesh.get_destinations(destmesh)
        if destinations is None:
            log("Error while fetching matrix streams")
            return None

        groups = []
        views = {}

        # For each source / dest combination, update the list of groups to
        # include any groups that should be queried to colour the
        # corresponding matrix cell.
        #
        # Note that there may be more than one group per cell, e.g. one half
        # of the cell may show ipv4 streams and the other may show ipv6
        # streams.
        for s in sources:
            for d in destinations:
                col.update_matrix_groups(s, d, groups, views, self.viewmanager)

        return groups, sources, destinations, views


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
