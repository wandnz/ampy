import time
import pylibmc

class AmpyCache(object):
    def __init__(self, blocksize):
        self.blocksize = blocksize
        self.memcache = pylibmc.Client(
                ["127.0.0.1"],
                behaviors={
                    "tcp_nodelay": True,
                    "no_block": True,
                })
        self.memcache.flush_all()
    
    def get_caching_blocks(self, stream, start, end, binsize, detail):
        blocks = []
        blocksize = binsize * self.blocksize

        prefetch = 2 * blocksize

        blockts = (start - (start % blocksize)) - prefetch
        now = int(time.time())

        while blockts < end + prefetch:
            if blockts > now:
                break

            if now < blockts + blocksize:
                cachetime = 300
            else:
                cachetime = 60 * 60 * 6

            key = str("_".join([str(binsize), str(blockts), str(stream),
                    str(detail)]))

            blocks.append({"start": blockts, "end": blockts + blocksize,
                    "binsize":binsize, "cachetime":cachetime, "cachekey":key})
            blockts += blocksize

        return blocks
 
    def search_cached_blocks(self, blocks):
        missing = []
        cached = {}

        current = {"start":0, "end":0, "binsize":0}

        for b in blocks:

            try:
                if b['cachekey'] in self.memcache:
                    cached[b['start']] = self.memcache.get(b['cachekey'])
                    continue
            except pylibmc.SomeErrors:
                pass

            # If we get here, the block wasn't in our cache
            if current['binsize'] == 0:
                # First block that we missed in the cache
                current['start'] = b['start']
                current['end'] = b['end']
                current['binsize'] = b['binsize']
                continue

            if b['start'] == current['end']:
                # This block is contiguous with the last missing block, 
                # combine them so we can deal with them in one query
                current['end'] = b['end']
                continue

            # If we get here, this block is disconnected from the previous
            # missing block(s). Finalise 'current' and start a new one based
            # on the current block.
            missing.append({'start':current['start'], 'end':current['end'],
                    'binsize':current['binsize']})
            
            current['start'] = b['start']
            current['end'] = b['end']
            current['binsize'] = b['binsize']

        # Check if we were still working on a missing block
        if current['binsize'] != 0:
            missing.append(current)

        return missing, cached


    def check_recent(self, query):
        key = str("_".join([str(query['stream']), str(query['duration']), 
                str(query['detail']), "recent"]))

        try:
            if key in self.memcache:
                return self.memcache.get(key)
        except pylibmc.SomeErrors:
            pass

        return []

    def store_block(self, block, blockdata):
        try:
            self.memcache.set(block['cachekey'], blockdata, block['cachetime'])
        except pylibmc.WriteError:
            pass

    def store_recent(self, query, result):

        # Brendon's hideous cache timeout calculation algorithm
        if query['duration'] <= (60 * 10):
            cachetime = 60
        elif query['duration'] <= (60 * 60):
            cachetime = 60 * 5
        elif query['duration'] <= (60 * 60 * 24):
            cachetime = 60 * 30
        elif query['duration'] <= (60 * 60 * 24 * 7):
            cachetime = 60 * 60 * 3
        else:
            cachetime = 60 * 60 * 6
        
        key = str("_".join([str(query['stream']), str(query['duration']), 
                str(query['detail']), "recent"]))
        try:
            self.memcache.set(key, result, cachetime)
        except pylibmc.WriteError:
            pass

    
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
