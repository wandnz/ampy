import time
import pylibmc

from libnntscclient.logger import *

class AmpyCache(object):
    """
    Class for storing and recalling ampy data via memcache.

    All cache operations should happen via this class.

    A quick note on terminology: a block is a sequence of time series data
    covering a short period (usually about 12 data points to a block). We
    cache at a block level to avoid filling up our cache with entries for
    specific data points while still being flexible enough that the cached
    data can be reused if a user pans or zooms the graph slightly.
    
    Blocks work well because a user will seldom want just a single data
    point in isolation; multiple consecutive data points are needed to plot
    a time series graph, for instance.

    API Functions
    -------------
      store_block:
        Caches a block of time series data.
      get_caching_blocks:
        Divides a time period into a series of blocks suitable for caching.
      search_cached_blocks:
        Given a list of blocks for a time series, finds all blocks that
        are present in the cache.
      search_ippaths:
        Searches the cache for IP path data for a given label.
      store_ippaths:
        Caches the result of an IP path query for a particular label.
      search_recent:
        Searches the cache for recent data for a given label.
      store_recent:
        Caches the result of a recent data query for a particular label.
      store_stream_view:
        Caches the view id that corresponds to a single stream.
      search_stream_view:
        Searches the cache for a view id matching a given stream ID.
      store_view_groups:
        Caches the groups that belong to a specific view ID.
      search_view_groups:
        Searches the cache for the list of groups that that belong to a given
        view ID.
   
   
    """ 
    def __init__(self, blocksize):
        """
        Init function for the AmpyCache class.

        Parameters:
          blocksize -- the number of binned measurements to include in a
                       data block.
        """

        self.blocksize = blocksize
        self.memcache = pylibmc.Client(
                ["127.0.0.1"],
                behaviors={
                    "tcp_nodelay": True,
                    "no_block": True,
                })
        self.mcpool = pylibmc.ThreadMappedPool(self.memcache)

        self.streamview_cachetime = 60 * 60 * 24
        self.viewgroups_cachetime = 60 * 60 * 6

    def __del__(self):
        """
        Del function for the AmpyCache class.
        """
        # Make sure we release any locks we have on the memcache
        self.mcpool.relinquish()

    def store_block(self, block, blockdata, label, binsize, detail, failed):
        """
        Caches a single data block for a label.

        Much of the complexity in this function comes from ensuring that we
        do not cache any results where the database query timed out.

        Parameters:
          block -- a dictionary describing the block itself, i.e. time
                   boundaries, how long to cache data for.
          blockdata -- a list of datapoints fetched for the label for this
                       block.
          label -- the label that the blockdata belongs to.
          binsize -- the aggregation frequency used when querying for the data.
          detail -- the detail level requested when querying for the data.
          failed -- a list of tuples describing time periods when queries
                    had timed out while fetching data for this label.

        Returns:
          an updated version of failed where any timeouts that had preceded
          the cached block have been removed.

        """

        cacheblock = True
        start = block['start']
        end = block['end']
        cachetime = block['cachetime']

        while len(failed) > 0:
            # The tuple format should be (start, end)
            nextfail = failed[0][0], failed[0][1]

            # If this block ends before the next timeout, cache it
            if nextfail[0] > end:
                break

            # If this block starts after the next timeout, that 
            # failure is no longer useful. Pop it and check the next one
            if nextfail[1] < start:
                failed = failed[1:]
                continue

            # Don't cache blocks which overlap with timed out queries
            if start >= nextfail[0] and start <= nextfail[1]:
                cacheblock = False
            elif end >= nextfail[0] and end <= nextfail[1]:
                cacheblock = False

        if not cacheblock:
            return failed
        
        # Cache the block  
        key = self._block_cache_key(start, binsize, detail, label)
        self._cachestore(key, blockdata, cachetime,
                "data block")

        return failed

    def get_caching_blocks(self, start, end, binsize, extra):
        """ 
        Divides a time period into fixed size blocks suitable for 
        caching.

        Parameters:
          start -- the start of the time period
          end -- the end of the time period
          binsize -- the aggregation frequency for the intended query
          extra -- the number of additional blocks to add either
                   side of the time period specified.

        For the purposes of caching, a block consists of N data
        points where N is the blocksize given when initialising the
        AmpyCache object. 

        Returns the list of blocks covering the specified time period
        at the given binsize (including any 'extra' blocks).
        """
        blocks = []
        blocksize = binsize * self.blocksize

        # Include 'extra' additional blocks either side when fetching data.
        # This is often used to ensure there will be data present if a
        # graph is panned.
        prefetch = extra * blocksize

        # Also, make sure our start and end times always fall on block
        # boundaries so we can easily match queried data to blocks.
        blockts = (start - (start % blocksize)) - prefetch

        now = int(time.time())
        while blockts < end + prefetch:
            if blockts > now:
                break

            # Only cache the most recent block for a short time as
            # there will probably be new measurements for that block soon
            if now < blockts + blocksize:
                cachetime = 300
            else:
                # Historical data isn't going to change so we can cache for
                # a while
                cachetime = 60 * 60 * 6

            blocks.append({'start':blockts, 'end':blockts + blocksize,
                    'cachetime':cachetime})
            blockts += blocksize

        return blocks

    def search_cached_blocks(self, blocks, binsize, detail, label):
        """
        Searches for any cached data that will satisfy the blocks in a given
        list.

        Parameters:
          blocks -- a list of dictionaries describing the blocks for which 
                    data is required.
          binsize -- the aggregation frequency for the time series.
          detail -- the level of detail required, e.g. 'full' vs 'matrix'.
          label -- the label for which data is required.

        Returns:
          a tuple containing two elements.
          The first element is a list of tuples describing blocks where there
          was no cached data available for the given label, binsize and detail
          level. The second element is a dictionary containing the cached data,
          where the key is the start-time of the block and the value is a
          list of datapoints for that block.
        """
        uncached = []
        cached = {}
        missing = 0

        nextblock = {"start":0, "end":0}

        for b in blocks:
            cachekey = self._block_cache_key(b['start'], binsize, detail, label)

            # Lookup the current block in memcache
            fetched = self._cachefetch(cachekey, "cached block")
            if fetched is not None:
                cached[b['start']] = fetched
                continue

            # Block was not in the cache
            missing += 1

            if missing == 1:
                # If true, this is the first block we've missed
                nextblock['start'] = b['start']
                nextblock['end'] = b['end']
                continue 

            if b['start'] == nextblock['end']:
                # This block is contiguous with the last uncached block.
                # Merge them so we can cover them both with one db query
                nextblock['end'] = b['end']
                continue

            # Otherwise, finalise the 'nextblock' and reset it to be based
            # on the current block
            uncached.append((nextblock['start'], nextblock['end']))
            nextblock['start'] = b['start']
            nextblock['end'] = b['end']

        # If we were still working on a uncached block, make sure we add it 
        # to the list
        if missing > 0:
            uncached.append((nextblock['start'], nextblock['end']))

        return uncached, cached

    def search_ippaths(self, label, start, end):
        """
        Searches the cache for the result of a IP Path query
        for a given label.

        Parameters:
          label -- the label for which recent data is required.
          start -- the start of the time period covered by the paths.
          end -- the end of the time period covered by the paths.

        Returns:
          a list of cached data points if the required data was in the
          cache, or None if the required data could not be found in the
          cache.
        """
        cachekey = self._ippath_cache_key(start, end, label)
        return self._cachefetch(cachekey, "IP paths")

    def store_ippaths(self, label, start, end, data):
        """
        Caches the result of a 'recent data' query for a label.

        Parameters:
          label -- the label which the recent data belongs to.
          start -- the start of the time period covered by the paths.
          end -- the end of the time period covered by the paths.
          data -- the result of the query.

        Returns:
          None
        """
        cachetime = 3 * 60 * 60
        cachekey = self._ippath_cache_key(start, end, label)

        self._cachestore(cachekey, data, cachetime,
                "IP paths")

    def search_recent(self, label, duration, detail):
        """
        Searches the cache for the result of a 'recent data' query
        for a given label.

        Parameters:
          label -- the label for which recent data is required.
          duration -- the amount of recent data required.
          detail -- the level of detail required for the recent data.

        Returns:
          a list of cached data points if the required data was in the
          cache, or None if the required data could not be found in the
          cache.
        """
        cachekey = self._recent_cache_key(label, duration, detail)
        return self._cachefetch(cachekey, "recent data")

    def store_recent(self, label, duration, detail, data):
        """
        Caches the result of a 'recent data' query for a label.

        Parameters:
          label -- the label which the recent data belongs to.
          duration -- the amount of recent data queried for.
          detail -- the level of detail that was queried for.
          data -- the result of the query.

        Returns:
          None
        """
        cachetime = self._recent_cache_timeout(duration)
        cachekey = self._recent_cache_key(label, duration, detail)

        self._cachestore(cachekey, data, cachetime,
                "recent data")

    def store_stream_view(self, streamid, viewid):
        """
        Caches the view ID that best matches a single stream ID.

        Stream to view mappings are useful for creating links to graphs 
        that show netevmon events, which do not have any concept of views
        or groups. Instead, we work out the most appropriate group for
        showing the series which the event detector used and create a view
        containing just that group.
        
        Rather than figuring out appropriate groups and views every time
        someone clicks on an event, we can cache the view ID for a given 
        stream the first time someone clicks on an event for that stream.

        Parameters:
          streamid -- the stream ID
          viewid -- the ID of the view that corresponds to the stream.

        Returns:
          None.
        """
        cachekey = self._stream_view_cache_key(streamid)
        
        self._cachestore(cachekey, viewid, self.streamview_cachetime,
                "stream view")

    def search_stream_view(self, streamid):
        """
        Searches the cache for a view ID that best matches a given stream ID.

        See store_stream_view for a detailed explanation of why we cache
        stream to view mappings.

        Parameters:
          streamid -- the stream ID to search for.

        Returns:
          a view ID if there is a cache entry for the given stream ID, None
          otherwise.
        """
        cachekey = self._stream_view_cache_key(streamid)
        return self._cachefetch(cachekey, "stream view")

    
    def store_view_groups(self, viewid, groups):
        """
        Caches the dictionary of groups that belong to a particular view.

        Parameters:
          viewid -- the ID number of the view that the groups belong to.
          groups -- the dictionary of groups for the view, as returned by
                    the get_view_groups() function in the ViewManager class.

        Returns:
          None
        """
        cachekey = self._view_groups_cache_key(viewid)
        self._cachestore(cachekey, groups, self.viewgroups_cachetime, 
                "view groups")

    def search_view_groups(self, viewid):
        """
        Searches the cache for the groups that belong to a given view.

        Parameters:
          viewid -- the ID number of the view to search for.

        Returns:
          a dictionary of groups if there is a cache entry for the given view
          ID, None otherwise.

        The returned dictionary matches the format returned by the 
        get_view_groups() function in the ViewManager class.
        """
        cachekey = self._view_groups_cache_key(viewid)
        return self._cachefetch(cachekey, "view groups")

    def _cachestore(self, key, data, cachetime, errorstr):
        """
        Internal helper function for storing a cache entry.

        Parameters:
          key -- the cache key to use
          data -- the data to be stored
          cachetime -- the length of time that the data should be cached,
                       in seconds.
          errorstr -- a string describing what is being cached for error
                      reporting purposes.

        Returns:
          None. If an error occurs, a warning will be printed and the data
          will not be cached but no further action will be taken.
        """
        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, data, cachetime)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while storing %s" % (errorstr))
                log(e)

    def _cachefetch(self, key, errorstr):
        """
        Internal helper function for finding a cache entry.

        Parameters:
          key -- the cache key to search for.
          errorstr -- a string describing what is being fetched for error
                      reporting purposes.

        Returns:
          None if no cache entry is found, otherwise the data stored using
          the given key.

        If an error occurs while searching the cache, a warning will be 
        printed and None will be returned.
        """

        result = None
        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    result = mc.get(key)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error when searching for %s" % (errorstr))
                log(e)

        return result

    # Functions to construct cache keys for the various types of data that
    # we cache. Hopefully, they do not need a full explanation.

    def _view_groups_cache_key(self, viewid):
        return "viewgroups_%s" % (str(viewid))

    def _stream_view_cache_key(self, streamid):
        return "streamview_%s" % (str(streamid))

    def _block_cache_key(self, start, binsize, detail, label):
        return str("_".join([label, str(binsize), str(start), str(detail)]))

    def _ippath_cache_key(self, start, end, label):
        return str("_".join([label, str(start), str(end)]))

    def _recent_cache_key(self, label, duration, detail):
        return str("_".join([label, "recent", str(duration), detail])) 

    def _recent_cache_timeout(self, duration):
        """
        Helper function to determine how long recent data should be cached
        for, based on the time period that the recent data covers.

        Parameters:
          duration -- the time period covered by the recent data, in seconds.

        Returns:
          The length of time to cache the recent data, in seconds.
        """

        # Brendon's hideous cache timeout calculation algorithm
        if duration <= 60 * 10:
            return 60
        if duration <= 60 * 60:
            return 60 * 5
        if duration <= 60 * 60 * 24:
            return 60 * 30
        if duration <= 60 * 60 * 24  * 7:
            return 60 * 60 * 3
        return 60 * 60 * 6

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
