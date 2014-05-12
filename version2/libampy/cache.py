import time
import pylibmc

from libnntscclient.logger import *

class AmpyCache(object):
    def __init__(self, blocksize):
        self.blocksize = blocksize
        self.memcache = pylibmc.Client(
                ["127.0.0.1"],
                behaviors={
                    "tcp_nodelay": True,
                    "no_block": True,
                })
        self.mcpool = pylibmc.ThreadMappedPool(self.memcache)

    def __del__(self):
        self.mcpool.relinquish()

    def is_valid(self):
        return True

    def store_block(self, block, blockdata, label, binsize, detail, failed):

        cacheblock = True
        start = block['start']
        end = block['end']
        cachetime = block['cachetime']

        while len(failed) > 0:
            print failed[0]
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

        key = self._block_cache_key(start, binsize, detail, label)

        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, blockdata, cachetime)
            except pylibmc.WriteError as e:
                log("Warning: Failed to cache data block %s")
                log(e)

        return failed

    def get_caching_blocks(self, start, end, binsize, extra):
        """ Divides a time period into fixed size blocks suitable for 
            caching.

            Parameters:
                start -- the start of the time period
                end -- the end of the time period
                binsize -- the aggregation frequency for the intended
                           query
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

    def search_cached_blocks(self, blocks, start, end, binsize, detail, label):
        uncached = []
        cached = {}
        missing = 0

        nextblock = {"start":0, "end":0}

        for b in blocks:
            cachekey = self._block_cache_key(b['start'], binsize, detail, label)

            # Look up the current block in memcache
            with self.mcpool.reserve() as mc:
                try:
                    if cachekey in mc:
                        cached[b['start']] = mc.get(cachekey)
                        continue
                except pylibmc.SomeErrors as e:
                    log("Warning: pylibmc error while searching for cached block")
                    log(e)
                    # Add this block to list of uncached blocks 
                    pass
           
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

    def search_recent(self, label, duration, detail):
        cachekey = self._recent_cache_key(label, duration, detail)

        result = None
        with self.mcpool.reserve() as mc:
            try:
                if cachekey in mc:
                    result = mc.get(cachekey)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while searching for recent data")
                log(e)

        return result

    def store_recent(self, label, duration, detail, data):
        cachetime = self._recent_cache_timeout(duration)
        cachekey = self._recent_cache_key(label, duration, detail)

        with self.mcpool.reserve() as mc:
            try:
                mc.set(cachekey, data, cachetime)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while inserting recent data")
                log(e)


    def _block_cache_key(self, start, binsize, detail, label):
        return str("_".join([label, str(binsize), str(start), str(detail)]))

    def _recent_cache_key(self, label, duration, detail):
        return str("_".join([label, "recent", str(duration), detail])) 

    def _recent_cache_timeout(self, duration):
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
