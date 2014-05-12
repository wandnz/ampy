from libnntscclient.logger import *

class StreamManager(object):
    def __init__(self, keylist):
        self.keylist = list(keylist)

        self.basedict = {}
        self.streams = {}
        self.activity = {}

    def add_stream(self, streamid, storage, properties):
        
        curr = self.basedict
        key = []

        for k in self.keylist:
            if k not in properties:
                return None
            val = properties[k]

            if val not in curr:
                if k == self.keylist[-1]:
                    curr[val] = []
                else:
                    curr[val] = {}

            key.append(val)
            curr = curr[val]

        # Should have a list at this point
        curr.append((streamid, storage))

        self.streams[streamid] = key, storage

        if 'firsttimestamp' in properties and 'lasttimestamp' in properties:
            self.activity[streamid] = {
                    'first':properties['firsttimestamp'],
                    'last':properties['lasttimestamp']
            }
        return curr

    def update_active_stream(self, streamid, timestamp):
        if streamid not in self.activity:
            self.activity[streamid] = {'first':timestamp, 'last':timestamp}
        else:
            self.activity[streamid]['last'] = timestamp

    def filter_active_streams(self, streams, start, end):
        return [s for s in streams \
                if self.activity[s]['last'] > start and \
                    self.activity[s]['first'] < end]

    def find_stream_properties(self, streamid):
        if streamid not in self.streams:
            return None

        return dict(zip(self.keylist, self.streams[streamid][0]))

    def find_streams(self, properties, searching=None, index=0, found=None):

        # Initialise our recursive search
        #
        # Note that the default found argument should NOT be an empty list
        # as any changes to found will persist in subsequent calls with
        # default arguments
        if searching is None:
            searching = self.basedict

        if found is None:
            found = []

        if index == len(self.keylist):
            found += searching
            return found

        key = self.keylist[index]

        if key in properties:
            val = properties[key]
            if val not in searching:
                return found
            found = self.find_streams(properties, searching[val], 
                    index + 1, found)
            return found

        for k, nextdict in searching.iteritems():
            found = self.find_streams(properties, nextdict, index + 1, found)
        
        return found

    def find_selections(self, selected):

        requested = None
        curr = self.basedict
        for k in self.keylist:

            if k not in selected:
                requested = k
                break

            val = selected[k]
            if val not in curr:
                log("Selected value %s for property %s is not present in the stream manager, invalid selection" % (val, k))
                return None

            curr = curr[val]

        if requested is None:
            # Reached the end of the dictionary
            return None, []

        return requested, curr.keys()

        
    


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
