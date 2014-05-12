import time
from threading import Lock

from libampy.nntsc import NNTSCConnection
from libampy.streammanager import StreamManager
from libnntscclient.protocol import *
from libnntscclient.logger import *

STREAM_CHECK_FREQ = 60 * 5
ACTIVE_CHECK_FREQ = 60 * 30

class Collection(object):

    def __init__(self, colid, viewmanager, nntscconf):
        self.viewmanager = viewmanager
        self.nntsc = NNTSCConnection(nntscconf)
        self.streammanager = None
        self.collection_name = "basecollection"
        self.streamproperties = None
        self.colid = colid

        self.lastchecked = 0
        self.lastactive = 0
        self.lastnewstream = 0
        self.collock = Lock()

    def create_group_description(self, options):
        return None        

    def parse_group_description(self, description):
        return None

    def stream_group_description(self, streamprops):
        return None

    def detail_columns(self, detail):
        return None

    def get_legend_label(self, description):
        return "No label"

    def group_to_labels(self, groupid, description, lookup=True):
        return []

    def format_single_data(self, data, freq):
        # For many collections, no formatting is required
        return data

    def format_list_data(self, datalist, freq):
        # For many collections, no formatting is required
        return datalist
        
    def find_stream(self, streamid):
        return self.streammanager.find_stream_properties(streamid)

    def update_matrix_groups(self, source, dest, options, groups):
        return groups

    def update_streams(self):

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
             
        self.collock.release()
        return now

    def get_selections(self, selected):
        result = {}
        repeat = True

        if self.streammanager is None:
            log("Error: no available streams for collection %s" % (self.collection_name))
            return None

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
        return stream, stream['stream_id']
    
    def filter_active_streams(self, streams, start, end):
        return self.streammanager.filter_active_streams(streams, start, end)

    def fetch_history(self, labels, start, end, binsize, detail):
        aggregators = self.detail_columns(detail)

        if aggregators is None:
            log("Failed to get aggregation columns for collection %s" % (self.collection_name))
            return None

        history = self.nntsc.request_history(self.colid, labels, start, end, 
                binsize, aggregators)
        if history is None:
            log("Failed to fetch history for collection %s" % (self.collection_name))
            return None

        return history

    def _fetch_streams(self):
        streams = self.nntsc.request_streams(self.colid, NNTSC_REQ_STREAMS,
                self.lastnewstream)

        if streams is None:
            log("Failed to query NNTSC for streams from collection %s" % (self.collection_name))
            return None

        if self.streammanager is None:
            self.streammanager = StreamManager(self.streamproperties)

        mostrecent = 0

        for s in streams:
            # Ignore streams that do not have any data, so as to avoid
            # the empty graph problem
            if s['lasttimestamp'] == 0:
                continue

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
        streams = self.nntsc.request_streams(self.colid, 
                NNTSC_REQ_ACTIVE_STREAMS, self.lastactive)

        if streams is None:
            log("Failed to query NNTSC for active streams from collection %s" % (self.collection_name))
            return None

        if streammanager is None:
            log("Error: streammanager should not be None when fetching active streams!")
            return none

        now = time.time()

        for s in streams:
            self.streammanager.update_active_stream(s, now)



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
