import time
import re
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
    get_maximum_view_groups:
        Returns the maximum number of groups that can be depicted on a single
        graph.
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
    get_collection_recent:
        Fetches aggregated recent data for a set of labels.
    get_collection_history:
        Fetches aggregated historical data for a set of labels.

    """

    def __init__(self, colid, viewmanager, nntscconf):
        self.viewmanager = viewmanager
        self.nntscconf = nntscconf
        self.streammanager = None
        self.colid = colid
        self.lastchecked = 0
        self.lastnewstream = 0
        self.collock = Lock()
        self.integerproperties = []

        # These members MUST be overridden by the child collection's init
        # function
        self.collection_name = "basecollection"
        self.streamproperties = None
        self.groupproperties = None

    def get_maximum_view_groups(self):
        """
        Provides the maximum number of groups that can be shown on a single
        graph for this collection.

        Some collections, e.g. amp-astraceroute, can only depict a single
        series so this function allows callers to get this information for
        any given collection.

        Returns:
          the maximum number of groups that can be shown on a graph, or
          zero if there is no limit.
        """

        return 0


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

    def calculate_binsize(self, start, end, detail):
        """
        Determines an appropriate binsize for a graph covering the
        specified time period.

        The default minimum binsize is 5 minutes. The default maximum
        binsize is 4 hours.

        Child collections should implement this if the default binsize
        algorithm is inappropriate for the typical stream for that
        collection. In particular, collections that measure more
        frequently than every 5 minutes should override this to provide
        a binsize closer to the measurement frequency.

        Parameters:
          start -- the start of the time period for the graph.
          end -- the end of the time period for the graph.
          detail -- the level of detail requested for the data.

        Returns:
          the recommended binsize in seconds
        """

        # Aim to have around 200 datapoints on a typical graph
        minbin = int(((end - start)) / 200)

        # Most collections measure at 5 min intervals so use this
        # as a minimum binsize
        if minbin <= 300:
            binsize = 300
        elif minbin <= 600:
            binsize = 600
        elif minbin <= 1200:
            binsize = 1200
        elif minbin <= 2400:
            binsize = 2400
        elif minbin <= 4800:
            binsize = 4800
        else:
            binsize = 14400

        return binsize

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
            log("Cannot convert list of properties for %s -- too many properties" % (self.collection_name))
            return None

        props = {}
        for i in range(0, len(options)):
            sp = proplist[i]
            if sp in self.integerproperties:
                props[sp] = int(options[i])
            else:
                props[sp] = self.convert_property(sp, options[i])

        return props

    def convert_property(self, streamprop, value):
        return value

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

    def group_columns(self, detail):
        """
        Given a requested level of detail, returns the columns that should
        be grouped during a database query.

        Only child collections that require grouping should implement this
        function.

        Common levels of detail are:
          'full' -- used for drawing the main graphs
          'matrix' -- used for getting aggregate numbers for the matrix cells
          'basic' -- used for drawing matrix tooltip graphs

        Parameters:
          detail -- the level of detail required

        Returns:
          a list of columns to append to a GROUP BY clause when making the
          database query.
        """

        return []

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
          a two-tuple. The first element is the legend label for the group, as
          a string. The second element is a string describing the aggregation
          method for the group.
        """

        return "No label", ""

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

    def format_single_data(self, data, freq, detail):
        """
        Modifies a single data point into a suitable format for display on
        a graph or a matrix.

        Child collections should implement this function if the data needs
        additional formatting beyond what is stored in the database.

        Parameters:
          data -- a dictionary containing the data point to be formatted
          freq -- the frequency that the measurements were collected at
          detail -- the level of detail required

        Returns:
          the updated data point dictionary
        """

        # For many collections, no formatting is required
        return data

    def format_list_data(self, datalist, freq, detail):
        """
        Modifies a list of data points into a suitable format for display on
        a graph or a matrix.

        Child collections should implement this function if the data needs
        additional formatting beyond what is stored in the database.

        Parameters:
          datalist -- a list of dictionaries containing the data points to
                      be formatted
          freq -- the frequency that the measurements were collected at
          detail -- the level of detail required

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

    def update_matrix_groups(self, source, dest, split, groups, views,
            viewmanager, viewstyle):
        """
        Finds all groups (and labels and streams) that must be queried to
        populate a matrix cell for this collection.

        Child collections that appear on the matrix MUST implement this
        function.

        Parameters:
          source -- the source for the matrix cell
          dest -- the destination for the matrix cell
          split -- the family or direction to show in the cell
          groups -- a dictionary containing all groups for the matrix so far
          views -- a dictionary mapping matrix cells to view ids
          viewstyle -- the view style to use when creating new groups / views.

        Returns:
          Nothing, but the groups dictionary is updated to contain the groups
          required for the source/destination cell. Each added group will
          contain a list of labels belonging to that group and each label will
          include a list of stream ids to query for that label.

          The views dictionary is also updated. In this case, an entry is
          added with the key (source, dest) and the value is a view id for
          the graph to show if the matrix cell is clicked on.

        """

        return

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

        self.collock.release()
        return now

    def get_selections(self, selected, term, page, pagesize, logmissing=True):
        """
        Given a set of known stream properties, finds the next possible
        decision point and returns the set of available options at that
        point. Any stream properties along the way that have only one
        possible choice are also returned.

        Child collections should NOT override this function.

        Selection options are divided into "pages", which are used to return a
        manageable number of options at a time. This allows callers to
        iteratively load more options as the user scrolls, rather than trying
        to load and insert all of the possible options into a dropdown at once
        which can be quite laggy if there are many possible options to present.

        Parameters:
          selected -- a dictionary of stream properties with known values
          term -- only options containing the 'term' string will be returned.
                  Set to "" to match all options.
          page -- a *string* representing the index of the page to return.
                  Pages are indexed starting from "1".
          pagesize -- the number of options to include in a page.
          logmissing -- if True, report an error message if any of the
                        selected options are not present in the streams
                        hierarchy.

        Returns:
          a dictionary where the key is a stream property and the value is
          a dictionary of possible choices for that stream property, given the
          properties already chosen in the 'selected' dictionary. The 'choices'
          dictionary contains two items:
            - 'maxitems', which lists the total number of options available for
              this property
            - 'items', a list of dictionaries describing each item. There are
              two fields in the dictionary: 'text' and 'id'.


        This function is best explained with an example. Consider amp-icmp,
        where there are 4 stream properties: source, dest, packetsize and
        family. The function is called with an empty term, a page size of 10
        and a page index of "1".

        If selected is empty, this function will return a dictionary of
        sources. If there are 22 total sources, we'll get something like:
            {'source': {'maxitems': 22, 'items': [ <first 10 sources> ]}}

        If selected contains an entry for 'source' but no 'dest' and there
        are 108 destinations for that source, the function instead returns
        something like:
            {'dest': {'maxitems': 108, 'items': [ <first 10 destinations for
                  the given source> ]}}

        Repeating the call with the same 'selected' but a page index of "2"
        will return the next 10 destinations, i.e. destinations 11-20.

        If selected contains both a 'source' and a 'dest' and that source/dest
        pair has used two different packet sizes, the result is:
            {'packetsize': {'maxitems': 2, 'items': [ {'text': '84',
                    'id': '84'}, {'text': '128', 'id': '128'} ]}}

        In the last case, instead imagine there is only one packet size
        available for the source/dest pair. In this case, this function adds
        the packet size to the result but will then automatically descend to
        the next level of the stream hierarchy, assuming that the only packet
        size was chosen, and includes the options at that level in the result
        as well.

        Descending through the hierarchy will continue until a stream property
        provides more than one possible choice or the bottom of the hierarchy
        is reached. In the packet size case, the result is something like:
            {'packetsize': {'maxitems': 1, 'items': [ {'text': "84",
                    'id': "84"} ]},
             'family': {'maxitems': 2, 'items': [ {'text': 'ipv4',
                    'id': 'ipv4'}, {'text': 'ipv6', id': 'ipv6'} ]}
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
            found = self.streammanager.find_selections(selected, term, page, pagesize, logmissing)
            if found is None:
                if logmissing:
                    log("Failed to get selection options for collection %s" % (
                                self.collection_name))
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
            if options['maxitems'] == 1:
                selected[key] = options['items'][0]['id']
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

    def get_collection_recent(self, cache, labels, duration, detail):
        """
        Function for querying NNTSC for 'recent' data, i.e. summary
        statistics for a recent time period leading up until the current time.

        If there is recent data for a queried label in the cache, that will be
        used. Otherwise, a query will be made to the NNTSC database.

        Parameters:
          cache -- the local memcache where fetched data should be cached
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
        timeouts = []
        querylabels = {}
        end = int(time.time())
        start = end - duration

        for lab in labels:
            # Check if we have recent data cached for this label
            # Attach the collection to the cache label to avoid matching
            # cache keys for both latency and hop count matrix cells
            cachelabel = "mtx_" + lab['labelstring'] + "_" + self.collection_name
            if len(cachelabel) > 128:
                log("Warning: matrix cache label %s is too long for memcache" % (cachelabel))

            cachehit = cache.search_recent(cachelabel, duration, detail)
            # Got cached data, add it directly to our result
            if cachehit is not None:
                recent[lab['labelstring']] = cachehit
                continue

            # Not cached, need to fetch it

            # If no streams were active, don't query for them. Instead
            # add an empty list to the result for this label.
            if len(lab['streams']) == 0:
                recent[lab['labelstring']] = []
            else:
                querylabels[lab['labelstring']] = lab['streams']

        if len(querylabels) > 0:
            # Fetch data for labels that weren't cached using one big
            # query
            result = self._fetch_history(querylabels, start, end, duration,
                    detail)
            if result is None:
                log("Error fetching history for matrix")
                return None

            for label, queryresult in result.iteritems():
                formatted = self.format_list_data(queryresult['data'], queryresult['freq'], detail)
                # Cache the result
                cachelabel = label + "_" + self.collection_name
                if len(cachelabel) > 128:
                    log("Warning: matrix cache label %s is too long for memcache" % (cachelabel))
                cache.store_recent(cachelabel, duration, detail, formatted)

                # Add the result to our return dictionary
                recent[label] = formatted

                # Also update the timeouts dictionary
                if len(queryresult['timedout']) != 0:
                    timeouts.append(label)

        return recent, timeouts



    def get_collection_history(self, cache, labels, start, end, detail,
            binsize):

        """
        Fetches historical data for a set of groups belonging to a provided
        collection.

        Parameters:
          cache -- the local memcache where fetched data should be cached
          labels -- a list of labels describing the groups to fetch data for
          start -- a timestamp describing the start of the historical period
          end -- a timestamp describing the end of the historical period
          detail --  the level of detail, e.g. 'full', 'matrix'. This will
                     determine which data columns are queried and how they
                     are aggregated.
          binsize -- the minimum desired aggregation frequency. -1 = no
                     aggregation, None = let ampy choose. Note that if ampy
                     suggests a larger binsize, then that will be preferred
                     over this value.


        Returns:
          a dictionary keyed by label where each value is a list containing
          the aggregated time series data for the specified time period.
          Returns None if an error occurs while fetching the data.
        """

        if binsize != -1:
            ampy_binsize = self.calculate_binsize(start, end, detail)

            if binsize is None or ampy_binsize > binsize:
                binsize = ampy_binsize

        # Break the time period down into blocks for caching purposes
        extra = self.extra_blocks(detail)
        blocks = cache.get_caching_blocks(start, end, binsize, extra)

        # Figure out which blocks are cached and which need to be queried
        notcached, cached = self._find_cached_data(cache, blocks, labels,
                binsize, detail)

        # Fetch all uncached data
        fetched = frequencies = timeouts = {}
        if len(notcached) != 0:
            fetch = self._fetch_uncached_data(notcached, binsize, detail)
            if fetch is None:
                return None

            fetched, frequencies, timeouts = fetch

        # Merge fetched data with cached data to produce complete series
        data = {}
        for label, dbdata in fetched.iteritems():
            data[label] = []
            failed = timeouts[label]

            for b in blocks:
                blockdata, dbdata = self._next_block(b, cached[label],
                    dbdata, frequencies[label], binsize, detail)
                data[label] += blockdata

                # Store this block in our cache for fast lookup next time
                # If it already is there, we'll reset the cache timeout instead
                failed = cache.store_block(b, blockdata, label, binsize,
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
                blockdata, ignored = self._next_block(b, cached[label],
                        [], 0, binsize, detail)
                data[label] += blockdata
                ignored = cache.store_block(b, blockdata, label, binsize,
                        detail, [])

        return data

    def _fetch_uncached_data(self, notcached, binsize, detail):
        """
        Queries NNTSC for time series data that was not present in the cache.

        Parameters:
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
            hist = self._fetch_history(labels, bstart, bend-1, binsize, detail)
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

    def _find_cached_data(self, cache, blocks, labels, binsize, detail):
        """
        Determines which data blocks for a set of labels are cached and
        which blocks need to be queried.

        Parameters:
          cache -- the cache to search
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

        for label in labels:
            # Check which blocks are cached and which are not
            missing, found = cache.search_cached_blocks(blocks,
                    binsize, detail, label['labelstring'])

            cached[label['labelstring']] = found

            # This skips the active stream filtering if the entire label is
            # already cached
            if len(missing) == 0:
                continue

            # Add missing blocks to the list of data to be fetched from NNTSC
            for b in missing:
                if b not in notcached:
                    notcached[b] = {label['labelstring']: label['streams']}
                else:
                    notcached[b][label['labelstring']] = label['streams']

        return notcached, cached

    def _next_block(self, block, cached, queried, freq, binsize, detail):
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
            incrementby = freq
            usekey = 'timestamp'
        else:
            incrementby = binsize
            usekey = 'binstart'

        # there won't be a valid increment value if we asked for raw data
        # and there is no data in this time period
        if incrementby < 1:
            return [], queried

        blockdata = []
        datum = {}
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
                    datum = self.format_single_data(queried[0], freq, detail)
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


    def _fetch_history(self, labels, start, end, binsize, detail):
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

        groupcols = self.group_columns(detail)
        if groupcols is None:
            log("Failed to get group columns for collection %s" % (self.collection_name))
            return None

        nntsc = NNTSCConnection(self.nntscconf)

        if detail == "matrix" or detail == "tooltiptext":
            history = nntsc.request_matrix(self.colid, labels, start, end,
                    aggregators)
        else:
            history = nntsc.request_history(self.colid, labels, start, end,
                    binsize, aggregators, groupcols)
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

        for s in streams:
            # Do any necessary tweaking to prepare the stream for storage
            # in our stream manager
            s, store = self.prepare_stream_for_storage(s)

            if self.streammanager.add_stream(s['stream_id'], store, s) is None:
                log("Failed to record new stream for collection %s" % (self.collection_name))
                log(s)
                continue

            if s['stream_id'] > self.lastnewstream:
                self.lastnewstream = s['stream_id']

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
