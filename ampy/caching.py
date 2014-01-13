import time
import pylibmc

class AmpyCache(object):
    def __init__(self, blocksize):
        """ Connect to memcache on initialisation """
        self.blocksize = blocksize
        self.memcache = pylibmc.Client(
                ["127.0.0.1"],
                behaviors={
                    "tcp_nodelay": True,
                    "no_block": True,
                })
        self.mcpool = pylibmc.ThreadMappedPool(self.memcache)

    def __del__(self):
        """ Close connection to memcache on deletion """
        self.mcpool.relinquish()

    def get_caching_blocks(self, stream, start, end, binsize, detail):
        """ Break a period into fixed sized blocks to help with caching """
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
        """ Determine which data blocks are present in the cache """
        missing = []
        cached = {}

        current = {"start":0, "end":0, "binsize":0}

        for b in blocks:

            with self.mcpool.reserve() as mc:
                try:
                    if b['cachekey'] in mc:
                        cached[b['start']] = mc.get(b['cachekey'])
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
        """ Fetch recent data for the matrix from the cache, if present """
        key = str("_".join([str(query['label']), str(query['duration']),
                str(query['detail']), "recent"]))

        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    return mc.get(key)
            except pylibmc.SomeErrors:
                pass

        return None

    def check_collection_streams(self, colid):
        """ Fetch the streams in a collection from the cache, if present """
        key = str("_".join(["colstreams", str(colid)]))

        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    return mc.get(key)

            except pylibmc.SomeErrors:
                pass

        return []

    def check_active_streams(self, colid):
        """ Fetch the active streams in a collection from the cache, if present
        """
        key = str("_".join(["activestreams", str(colid)]))

        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    return mc.get(key)

            except pylibmc.SomeErrors:
                pass

        return []

    def check_streaminfo(self, streamid):
        """ Fetch stream information from the cache, if present """
        key = str("_".join(["streaminfo", str(streamid)]))

        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    return mc.get(key)

            except pylibmc.SomeErrors:
                pass

        return {}

    def store_streaminfo(self, stream, streamid):
        """ Cache the information for a particular stream """
        infokey = str("_".join(["streaminfo", str(streamid)]))

        with self.mcpool.reserve() as mc:
            try:
                # Individual streams are very unlikely to change, so we can
                # hang onto the stream info for a while
                mc.set(infokey, stream, 60 * 60 * 24)
            except pylibmc.WriteError:
                pass

    def store_collection_streams(self, colid, streams):
        """ Cache the streams in a collection """
        idkey = str("_".join(["colstreams", str(colid)]))

        with self.mcpool.reserve() as mc:
            try:
                # Expire this reasonably frequently, so we can learn about
                # new or deleted streams. Our main goal is to avoid making
                # multiple near-simultaneous requests for the same data
                # because our ajax requests get spread across multiple
                # processes.
                mc.set(idkey, streams, 60 * 30)
            except pylibmc.WriteError:
                pass

    def store_active_streams(self, colid, streams):
        """ Cache the active streams in a collection """
        idkey = str("_".join(["activestreams", str(colid)]))

        with self.mcpool.reserve() as mc:
            try:
                # TODO is 30 minutes fine?
                mc.set(idkey, streams, 60 * 30)
            except pylibmc.WriteError:
                pass

    def store_block(self, block, blockdata):
        """ Cache a block of fetched data """
        with self.mcpool.reserve() as mc:
            try:
                mc.set(block['cachekey'], blockdata, block['cachetime'])
            except pylibmc.WriteError:
                pass

    def store_recent(self, query, result):
        """ Cache recent (non-block) data that the matrix displays """
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

        key = str("_".join([str(query['label']), str(query['duration']),
                str(query['detail']), "recent"]))

        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, result, cachetime)
            except pylibmc.WriteError:
                pass


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
