import time, re
from threading import Lock

from libampy.nntsc import NNTSCConnection
from libampy.streammanager import StreamManager
from libnntscclient.protocol import *
from libnntscclient.logger import *

STREAM_CHECK_FREQ = 60 * 5
ACTIVE_CHECK_FREQ = 60 * 30

class Collection(object):
    """
    Base class for all collection modules.

    Each collection module will inherit from this class, which provides
    functionality that is common to all collections.

    In some cases, this class will provide a basic method that works
    for most collections but can be overridden if a collection requires
    something different, e.g. format_single_data. There are also 
    abstract functions that must be implemented by each collection, such as
    parse_group_description.

    API Functions 
    -------------
    extra_blocks:
        Determines how many extra blocks should be fetched when making
        a data query.
    create_group_from_list:
        Converts an ordered list of group properties into an appropriate
        group description for the collection.
    create_properties_from_list:
        Converts an ordered list of group properties into a dictionary
        keyed by the property name.
    translate_group:
        Converts the properties that describe a group from another
        collection into a string describing an equivalent group for this
        collection. 
    parse_group_description:
        Converts a group description string into a dictionary mapping
        group properties to their values.
    create_group_description:
        Converts a dictionary of stream or group properties into a string
        describing the group.
    detail_columns:
        Determines which data table columns should be queried and how they 
        should be aggregated, given the amount of detail required by the
        user.
    get_legend_label:
        Converts a group description string into an appropriate label for
        placing on a graph legend.
    group_to_labels:
        Converts a group description string into a set of labels describing
        each of the lines that would need to be drawn on a graph for that
        group.
    format_single_data:
        Modifies a single queried data point into an appropriate format for
        display on a graph, e.g. converting byte counts into bitrates.
    format_list_data:
        Similar to format_single_data, except it modifies an entire list of
        data points.
    find_stream:
        Finds the stream that matches the given stream id and returns a
        dictionary of stream properties that describe it.
    update_matrix_groups:
        Finds all of the groups that need to queried to populate a matrix cell,
        including the stream ids of the group members.
    update_streams:
        Queries NNTSC for any new streams that have appeared since we last
        updated our local stream manager. 
    get_selections:
        Given a set of known stream properties, finds the next possible
        decision point and returns the set of available options at that
        point. Any stream properties along the way that have only one
        possible choice are also returned.
    prepare_stream_for_storage:
        Performs any necessary conversions on the stream properties so
        that it can be inserted into a stream manager hierarchy.
    filter_active_streams:
        Filters a list of streams to only contain streams that were active
        during a particular time period.
    fetch_history:
        Fetches aggregated historical data for a set of labels.

    """

    def __init__(self, colid, viewmanager, nntscconf):
        self.viewmanager = viewmanager
        self.nntscconf = nntscconf
        self.streammanager = None
        self.colid = colid
        self.lastchecked = 0
        self.lastactive = 0
        self.lastnewstream = 0
        self.collock = Lock()
        
        # These members MUST be overridden by the child collection's init
        # function
        self.collection_name = "basecollection"
        self.streamproperties = None
        self.groupproperties = None

    def extra_blocks(self, detail):
        """
        Determines how many extra blocks are required either side of the
        requested time period, based on the requested level of detail.

        Queries for the detailed graph should fetch extra blocks. 
        Queries for the summary graph, tooltips or other non-scrolling
        graphs should not fetch any extra blocks.

        Child collections only need to implement this if they are not
        using the full/summary details used by the basic time series
        class in amp-web.

        Parameters:
          detail -- the level of detail requested for the data

        Returns:
          the number of extra blocks to add to each side of the 
          requested time period.
        """
        if detail == "full":
            return 2

        return 0

    def create_group_from_list(self, options):
        """
        Converts an ordered list of group properties into a suitable group
        description string.

        This is mainly to support creating groups via the web API.

        Parameters:
          options -- the list of properties describing the group. The
                     properties MUST be in the same order as they are
                     listed in the groupproperties list for the collection.

        Returns:
          a string describing the group or None if no string can be formed
          using the provided property list
        """
        
        props = self.create_properties_from_list(options, self.groupproperties)
        if props is None:
            return None
        return self.create_group_description(props) 

    def create_properties_from_list(self, options, proplist):
        """
        Converts an ordered list of group properties into a dictionary 
        with the property names as keys.

        Parameters:
          options -- the list of properties describing the group. The
                     properties MUST be in the same order as they are
                     listed in proplist.
          proplist -- the list of group properties, in order.
        
        Returns:
          a dictionary describing the group or None if no dictionary 
          can be formed using the provided property list
        """
        if proplist is None:
            # Child collection hasn't provided any group property list!
            return None

        if len(options) > len(proplist):
            log("Cannot convert list of properties -- too many properties")
            return None

        props = {}
        for i in range(0, len(options)):
            sp = proplist[i]
            props[sp] = options[i]

        return props
    def translate_group(self, groupprops):
        """
        Attempts to create a group description string based on a set of 
        properties describing a group from another collection.

        This is used by the graphs to generate tabs that link to graphs
        that may be related to the one currently shown, e.g. when looking
        at a latency graph, it is nice to be able to quickly switch to a
        traceroute for that path.

        All child collections MUST implement this function unless the
        collection is not related to any other collections. 

        Parameters:
          groupprops -- a dictionary describing the properties of the
          group from the other collection.

        Returns:
          a string describing an equivalent group from this collection, or
          None if no sensible equivalent group exists.
        """
        return self.create_group_description(groupprops)

    def parse_group_description(self, description):
        """
        Converts a group description string into a dictionary of group
        properties.

        All child collections MUST implement this function.

        Parameters:
          description -- the group description string to be converted

        Returns:
          a dictionary of group properties or None if the string does
          not match the correct format for the collection.
        """

        return None

    def create_group_description(self, streamprops):
        """
        Converts a dictionary of group or stream properties into a
        group description string.

        If the provided dictionary describes a stream, any group properties
        not present will be estimated -- e.g. the 'aggregation' property
        for AMP groups.

        All child collections MUST implement this function.

        Parameters:
          streamprops -- the dictionary of group or stream properties

        Returns:
          a string describing a group that best reflects the properties
          provided. Returns None if required properties are missing or
          conversion is otherwise impossible.
        """

        return None

    def detail_columns(self, detail):
        """
        Given a requested level of detail, returns the columns that should
        be queried from a collection's data table.
        
        All child collections MUST implement this function.

        Common levels of detail are:
          'full' -- used for drawing the main graphs
          'matrix' -- used for getting aggregate numbers for the matrix cells
          'basic' -- used for drawing matrix tooltip graphs

        Parameters:
          detail -- the level of detail required

        Returns:
          a two-tuple. The first element is the list of columns to query. The
          second element is the list of aggregation functions to apply to each
          column. Returns None in the event of an error.
        """

        return None

    def get_legend_label(self, description):
        """
        Generates a suitable label to display on a graph legend for a group.

        All child collections MUST implement this function.

        Parameters:
          description -- a string describing the group

        Returns:
          the legend label for the group, as a string
        """  
         
        return "No label"

    def group_to_labels(self, groupid, description, lookup=True):
        """
        Returns a set of labels describing the lines that need to be drawn
        on a graph for a group.

        All child collections MUST implement this function.
        
        Each label is a dictionary with three elements:
          labelstring: a unique string identifying this label
          streams: a list of stream ids belonging to the label. May be empty
                   if lookup was False.
          shortlabel: a short textual label distinguishing this label from
                      others for the same group. Useful for tooltips.

        Parameters:
          groupid -- the unique ID number for the group
          description -- the string describing the group
          lookup -- if False, the function will not lookup the streams 
                    for the group unless required for labelling purposes.
                    Otherwise, each label will include a list of stream
                    ids belonging to that label.

        Returns:
          a list of labels belonging to the group. 
        """

        return []

    def format_single_data(self, data, freq):
        """
        Modifies a single data point into a suitable format for display on
        a graph or a matrix.

        Child collections should implement this function if the data needs
        additional formatting beyond what is stored in the database.

        Parameters:
          data -- a dictionary containing the data point to be formatted
          freq -- the frequency that the measurements were collected at

        Returns:
          the updated data point dictionary
        """

        # For many collections, no formatting is required
        return data

    def format_list_data(self, datalist, freq):
        """
        Modifies a list of data points into a suitable format for display on 
        a graph or a matrix.

        Child collections should implement this function if the data needs
        additional formatting beyond what is stored in the database.

        Parameters:
          datalist -- a list of dictionaries containing the data points to 
                      be formatted
          freq -- the frequency that the measurements were collected at

        Returns:
          the updated data point dictionary
        """

        # For many collections, no formatting is required
        return datalist
        
    def find_stream(self, streamid):
        """
        Finds the stream that matches the provided stream id

        Child collections should NOT implement this function.

        Parameters:
          streamid -- the ID of the stream being searched for

        Returns:
          a dictionary containing the properties that describe the stream
          with the requested ID. Returns None if no stream with the
          requested ID exists for the collection.
        """

        return self.streammanager.find_stream_properties(streamid)

    def update_matrix_groups(self, source, dest, options, groups):
        """
        Finds all groups (and labels and streams) that must be queried to
        populate a matrix cell for this collection.

        Child collections that appear on the matrix MUST implement this
        function.

        Parameters:
          source -- the source for the matrix cell
          dest -- the destination for the matrix cell
          options -- an ordered list of additional group properties for the
                     matrix cell
          groups -- a dictionary containing all groups for the matrix so far

        Returns:
          an updated groups dictionary now containing the groups required for 
          the source/destination cell. Each added group will contain a list of
          labels belonging to that group and each label will include a list
          of stream ids to query for that label.

        The options parameter can be used to limit a cell to only groups
        matching a certain set of group properties. All group properties
        other than the 'source' and 'destination' should be present in this
        list and the options must be provided in the order they appear in 
        the groupproperties list.

        """
        
        return groups

    def update_streams(self):
        """
        Fetches new streams from NNTSC if the stream manager has not been
        updated recently.

        Child collections should NOT override this function.

        Returns:
          None if an error occurs while fetching streams, otherwise returns
          the current timestamp.
        """

        # XXX Do I need to wrap this in a Lock?
        self.collock.acquire()
        now = time.time()

        if now >= (self.lastchecked + STREAM_CHECK_FREQ):
            if self._fetch_streams() is None:
                self.collock.release()
                return None

            # Account for time taken querying for streams
            self.lastchecked = time.time()
        
        if now >= (self.lastactive + ACTIVE_CHECK_FREQ):
            if self._fetch_active_streams() is None:
                self.collock.release()
                return None

            # Account for time taken querying for streams
            self.lastactive = time.time()
             
        self.collock.release()
        return now

    def get_selections(self, selected):
        """
        Given a set of known stream properties, finds the next possible
        decision point and returns the set of available options at that
        point. Any stream properties along the way that have only one
        possible choice are also returned.

        Child collections should NOT override this function.
        

        Parameters:
          selected -- a dictionary of stream properties with known values

        Returns:
          a dictionary where the key is a stream property and the value is
          a list of possible choices for that stream property, given the
          properties already chosen in the 'selected' dictionary. 

        This function is best explained with an example. Consider amp-icmp,
        where there are 4 stream properties: source, dest, packetsize and
        family.

        If selected is empty, this function will return a dictionary:
            {'source':[ <all possible sources> ]}

        If selected contains an entry for 'source' but no 'dest', the
        function instead returns something like:
            {'dest': [ <all destinations for the given source> ]}
         
        If selected contains both a 'source' and a 'dest', the result is:
            {'packetsize': [ <all packet sizes for the given source/dest pair ]}

        In the last case, imagine there is only one packet size available for
        the source/dest pair. In this case, this function adds the packet size
        to the result but will then automatically descend to the next level of 
        the stream hierarchy, assuming that the only packet size was chosen,
        and include the options at that level in the result as well.
        
        Descending through the hierarchy will continue until a stream property
        provides more than one possible choice or the bottom of the hierarchy
        is reached. In the packet size case, the result is something like:
            {'packetsize':[ one size ], 
             'family': [ all possible families for source/dest/size ]
            }

        """
        result = {}
        repeat = True

        # Make sure we actually have a stream manager
        if self.streammanager is None:
            log("Error: no available streams for collection %s" % (self.collection_name))
            return None

        # Stupid python not having a do-while construct
        while repeat:
            found = self.streammanager.find_selections(selected)
            if found is None:
                log("Failed to get selection options for collection %s" % (self.collection_name))
                return found

            key, options = found
            if key is not None:
                result[key] = options
            else:
                # A None key means that we reached the end of the selection
                # list
                break

            # If there is only one possible option, why not automatically
            # assume it will be selected and fetch the next level of options
            # since the caller will probably want to do that anyway
            if len(options) == 1:
                selected[key] = options[0]
            else:
                repeat = False

    
        return result

    def prepare_stream_for_storage(self, stream):
        """
        Adds any missing stream properties to the stream dictionary provided
        by the database.

        Also, creates any user data that is to be stored with the stream in
        the stream manager.

        Examples of missing stream properties are address families; most
        streams include an address but we often group streams by address
        family so need to add a 'family' field to the stream.

        Some collections require the target IP address to be stored with the 
        stream so that we can easily label graphs where we plot one line per
        observed IP address. Therefore, we allow collections to store 
        additional data alongside the stream ID.

        Child collections are not required to implement this, but should do
        so if there are stream properties that do not exist as columns in
        the collection streams table in NNTSC.

        Parameters:
          stream -- a dictionary describing the stream that is about to be 
                    inserted into the stream hierarchy

        Returns:
         a two-tuple. The first element is the updated stream dictionary.
         The second element is the data to be stored with the stream in
         the stream manager.

        """

        # None is a special value for storage -- streams with None storage
        # will be stored as a single ID value rather than a tuple of 
        # (ID, storage data).
        return stream, None
    
    def filter_active_streams(self, streams, start, end):
        """
        Removes all entries in a streams list that were not active during 
        the specified time period.

        Parameters:
          streams -- a list of stream IDs
          start -- a timestamp indicating the start of the time period
          end -- a timestamp indicating the end of the time period

        Returns:
          a modified streams list with all inactive streams removed.
        """
        return self.streammanager.filter_active_streams(streams, start, end)

    def fetch_history(self, labels, start, end, binsize, detail):
        """
        Queries NNTSC for aggregated historical data for a set of labels.

        Parameters:
          labels -- a dictionary describing the labels to query for.
          start -- the start of the time period to get data for
          end -- the end of the time period to get data for
          binsize -- the frequency at which data should be aggregated
          detail -- a string describing the level of detail to use when 
                    querying

        Returns:
          a dictionary containing the results of the query. The dictionary 
          format is explained below.

        Label dictionary format:
          The key should be a unique string identifying the label.
          The value should be a list of stream IDs that belong to the label,
            e.g. {'group_50_ipv4':[12, 14, 16], 'group_50_ipv6':[22,34,10]}
          No stream ID should belong to more than one label.

        Detail:
          Different detail levels will result in different columns and
          aggregation methods being used. See detail_columns() for more
          information.

        Result dictionary format:
          The key is a string matching the label.
          The value is a dictionary containing the query results for that
          label. The value dictionary contains three elements of interest:
                'data': 
                    a list of data points. Each data point is itself a 
                    dictionary representing a row from the query result.
                'freq':
                    an integer that is an estimate of the measurement frequency
                'timedout':
                    a list of tuples describing queried time periods where the
                    query failed to complete in time. The tuple has the
                    format (start, end).

        """

        # Figure out which columns we need to query and how we query them
        aggregators = self.detail_columns(detail)

        if aggregators is None:
            log("Failed to get aggregation columns for collection %s" % (self.collection_name))
            return None
        nntsc = NNTSCConnection(self.nntscconf)
        history = nntsc.request_history(self.colid, labels, start, end, 
                binsize, aggregators)
        if history is None:
            log("Failed to fetch history for collection %s" % (self.collection_name))
            return None

        return history

    def _fetch_streams(self):
        """
        Asks NNTSC for all streams that we have never seen before.

        Returns:
          the number of new streams or None if an error occurs.
        """
        nntsc = NNTSCConnection(self.nntscconf)
        streams = nntsc.request_streams(self.colid, NNTSC_REQ_STREAMS,
                self.lastnewstream)

        if streams is None:
            log("Failed to query NNTSC for streams from collection %s" % (self.collection_name))
            return None

        # Create a stream manager to keep track of all these streams
        if self.streammanager is None:
            self.streammanager = StreamManager(self.streamproperties)

        mostrecent = 0

        for s in streams:
            # Ignore streams that do not have any data, so as to avoid
            # the empty graph problem
            if s['lasttimestamp'] == 0:
                continue

            # Do any necessary tweaking to prepare the stream for storage
            # in our stream manager
            s, store = self.prepare_stream_for_storage(s)

            if self.streammanager.add_stream(s['stream_id'], store, s) is None:
                log("Failed to record new stream for collection %s" % (self.collection_name))
                log(s)
                continue

            if s['stream_id'] > self.lastnewstream:
                self.lastnewstream = s['stream_id']

            if s['lasttimestamp'] > mostrecent:
                mostrecent = s['lasttimestamp']

        if self.lastactive == 0:
            self.lastactive = mostrecent
        return len(streams)

    def _fetch_active_streams(self):
        """
        Fetch any streams that have been active since the last time we
        checked.

        Returns:
          the number of active streams, or None if an error occurs while
          fetching streams.

        """
        nntsc = NNTSCConnection(self.nntscconf)
        streams = nntsc.request_streams(self.colid, 
                NNTSC_REQ_ACTIVE_STREAMS, self.lastactive)

        if streams is None:
            log("Failed to query NNTSC for active streams from collection %s" % (self.collection_name))
            return None

        if self.streammanager is None:
            log("Error: streammanager should not be None when fetching active streams!")
            return None

        now = time.time()

        for s in streams:
            self.streammanager.update_active_stream(s, now)
        return len(streams)

    def _address_to_family(self, address):
        """
        Handy helper function for converting an IP address to a string
        describing the address family it belongs to.

        Used by a number of collections, so it is part of the parent class.
        """
        if '.' in address:
            return 'ipv4'
        else:
            return 'ipv6'

    def _apply_group_regex(self, regex, description):
        """
        Attempts to use a regular expression to deconstruct a group
        description.

        Parameters:
          regex -- the regular expression to apply to the description
          description -- the group description string

        Returns:
          A re.MatchObject instance that results from matching the regular
          expression against the string. Returns None if the description
          does not match the regular expression.
        """

        parts = re.match(regex, description)
        if parts is None:
            log("Group description did not match regex for %s" % \
                    (self.collection_name))
            log(description)
            return None

        return parts

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
