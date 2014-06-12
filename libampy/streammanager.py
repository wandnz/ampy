from libnntscclient.logger import *

class StreamManager(object):
    """
    Class for managing stream hierarchies.

    A stream hierarchy is a series of nested dictionaries that can be used
    to find streams that match a given set of selection criteria. It is also
    used to find possible selection options based on earlier selections, e.g.
    fetching the destinations for a selected source.
    
    Searching is also supported in the opposite direction, so that the
    properties can be found for a given stream id.

    API Functions
    -------------
    add_stream:
        Adds a new stream to the hierarchy.
    update_active_stream:
        Updates the times that a stream was known to be active.
    filter_active_streams:
        Modifies a list of streams to only contain streams that were active
        during a given time period.
    find_stream_properties:
        Returns the set of stream properties that describe the stream matching
        a given id.
    find_streams:
        Returns all stream ids that match the stream properties provided.
    find_selections:
        Returns all possible values for the next level of the hierarchy, given
        the selections made at earlier levels.


    """

    def __init__(self, keylist):
        """
        Init function for the StreamManager class

        Parameters:
            keylist -- an ordered list of stream properties. The first of the
                       properties will be the top level of the hierarchy and
                       each subsequent property will be the next level.

        The keylist should be ordered in the way you would expect users to
        make selections on the modal dialog for the collection. 
        
        An example keylist for AmpIcmp would be:
        ['source', 'destination', 'packet_size', 'family']
        """
        self.keylist = list(keylist)

        self.basedict = {}
        self.streams = {}
        self.activity = {}

    def add_stream(self, streamid, storage, properties):
        """
        Adds a new stream to the existing hierarchy

        Parameters:
          streamid -- the id number of the stream being added
          storage -- any additional data that should be stored with the 
                     streamid in the hierarchy, e.g. an IP address for ICMP
                     streams. If None, no extra storage is used.
          properties -- a dictionary describing the stream properties for the
                        stream being added. All stream properties should be
                        present in the dictionary otherwise the insertion
                        will fail.

        Returns:
          None if the stream could not be added to the hierarchy, otherwise
          returns the list of streams that the new stream was added to.
        """

        curr = self.basedict
        key = []

        # Iterate through our nested dictionaries, creating new entries if
        # none exist for the various stream properties. Once we get to the
        # end of the hierarchy we should be pointing at a list of stream
        # ids that match all of the preceding properties.
        for k in self.keylist:
            # Make sure all of the expected properties are present
            if k not in properties:
                return None
            val = properties[k]

            if val not in curr:
                # We're at the end of the hierarchy, so create a new list
                # instead.
                if k == self.keylist[-1]:
                    curr[val] = []
                else:
                    curr[val] = {}

            key.append(val)

            # Move down to the next hierarchy level
            curr = curr[val]

        # Should have a list at this point, so append our new stream id and
        # any 'extra' data we need to keep here
        if storage is not None:
            curr.append((streamid, storage))
        else:
            curr.append(streamid)

        # Also update our streamid -> streamprops dictionary so we can
        # look up streams by id as well.
        self.streams[streamid] = key, storage

        # Remember when this stream was active too, so we can filter out
        # inactive streams easily   
        if 'firsttimestamp' in properties and 'lasttimestamp' in properties:
            self.activity[streamid] = {
                    'first':properties['firsttimestamp'],
                    'last':properties['lasttimestamp']
            }
        return curr

    def update_active_stream(self, streamid, timestamp):
        """
        Updates the entry for a stream in the stream activity map.

        Parameters:
          streamid -- the id of the stream to be updated
          timestamp -- the time that the stream was observed to be active

        """

        if streamid not in self.activity:
            # It's a new stream, create a new map entry
            self.activity[streamid] = {'first':timestamp, 'last':timestamp}
        else:
            # Otherwise, just update the most recent timestamp
            self.activity[streamid]['last'] = timestamp

    def filter_active_streams(self, streams, start, end):
        """
        Given a list of stream ids, removes all streams that were not active
        during a specified time period.

        Parameters:
          streams -- the list of stream ids to be filtered
          start -- the start of the time period of interest
          end -- the end of the time period of interest

        Returns:
          a new list of stream ids, with the inactive streams removed.
        """
        return [s for s in streams \
                if self.activity[s]['last'] > start and \
                    self.activity[s]['first'] < end]

    def find_stream_properties(self, streamid):
        """
        Returns the properties required to find the given stream id in 
        the stream hierarchy.

        In other words, returns the stream properties for the given stream id.

        Parameters:
          streamid -- the id of the stream to find the properties for.

        Returns:
            None if the stream id is not in the hierarchy, otherwise a 
            dictionary of properties to values for the stream.
        """
        if streamid not in self.streams:
            return None

        return dict(zip(self.keylist, self.streams[streamid][0]))

    def find_streams(self, properties, searching=None, index=0, found=None):
        """
        Finds all streams that match a given set of stream properties.

        Note: this is a recursive function. If you are calling this 
        function, make sure that you do NOT provide values for any parameters
        other than the 'properties' parameter.

        Parameters:
          properties -- a dictionary containing the stream properties 
          searching -- the hierarchy dictionary that is currently being
                       searched. If None, the function will search at the
                       top hierarchy level.
          index -- a number indicating how many recursive calls we have made
                   thus far.
          found -- a list containing the stream ids that have been matched
                   thus far. If None, a new list will be created.

        Returns:
            a list of streams that matched the given criteria. If there
            was additional data stored with the stream ID, the list items
            are tuples where the first element of the tuple is the stream id 
            and the second is the stored 'extra' data for the stream.
            Otherwise, the list items are just stream ids (no tuple at all).

        The properties dictionary is not required to contain all of the
        stream property keys. If a key is missing, all of the dictionaries
        at that level will be traversed and searched using the remaining
        properties. This means it is possible to do broad searches, i.e.
        get all of the ICMP streams for a source and destination regardless
        of the packet_size or address family.

        """

        # Initialise our recursive search
        #
        # Note that the default found argument should NOT be an empty list
        # as any changes to found will persist in subsequent calls with
        # default arguments
        if searching is None:
            searching = self.basedict

        if found is None:
            found = []

        # In this case, we've reached the end of the hierarchy and can just
        # tack on whatever list of streams is here
        if index == len(self.keylist):
            found += searching
            return found

        key = self.keylist[index]

        if key in properties:
            # There is a specific value for the current stream property
            val = properties[key]

            #print key, val
            #print searching.keys()

            # No entry for this property value in hierarchy, so we can bail
            if val not in searching:
                return found

            # Recurse down to the next hierarchy level    
            found = self.find_streams(properties, searching[val], 
                    index + 1, found)
            return found

        # If we get here, the stream property at this level was not in the
        # provided set of parameters so we will treat it as a wildcard 
        # and traverse all of the entries at this hierarchy level
        for k, nextdict in searching.iteritems():
            found = self.find_streams(properties, nextdict, index + 1, found)
        
        return found

    def find_selections(self, selected, logmissing=True):
        """
        Gets a list of possible values at a hierarchy level, given a set of
        selections made at earlier levels.

        This function is used to populate the dropdowns in the modal dialogs.

        Parameters:
          selected -- a dictionary containing all of the currently selected
                      stream properties.
          logmissing -- if True, log an error message if a selection option
                        is not present in the hierarchy.

        Returns:
          a tuple of two items : the name of the stream property being 
          returned and the list of possible values for that property.

        Example:
          AMP ICMP has four stream properties: source, dest, size and family.
          To get the list of possible sources, selected should be an empty
          dictionary. To get the list of destinations for a given source S,
          selected should be {'source':S}. To get the list of packet sizes for
          a given source S and dest D, selected should be {'source':S, 
          'dest':D} etc. etc.

        This function will always return a list of values from the highest
        hierarchy level which is not present in the 'selected' parameter,
        i.e. all preceding stream properties must be provided to get a list
        of possible values at a particular level.
        """
        
        requested = None
        curr = self.basedict

        # Iterate through selected to find the appropriate hierarchy level
        for k in self.keylist:

            # The key at this level is not present in selected, so we've gone
            # as far as we can
            if k not in selected:
                requested = k
                break

            val = selected[k]

            # Convert boolean strings to actual boolean values if needed
            if val not in curr and val == "true":
                val = True
            if val not in curr and val == "false":
                val = False

            # Make sure the selected value for this level is actually valid
            if val not in curr:
                if logmissing:
                    log("Selected value %s for property %s is not present in the stream manager, invalid selection" % (val, k))
                return None

            curr = curr[val]

        if requested is None:
            # Reached the end of the hierarchy, make sure we don't 
            # accidentally return the stream id list
            return None, []

        return requested, curr.keys()

        
    


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
