import socket, sys
from libnntscclient.protocol import *
from libnntscclient.nntscclient import NNTSCClient
from libnntscclient.logger import *

class NNTSCConnection(object):
    """
    Class for querying a NNTSC database.

    API Functions
    -------------
    request_collections:
        Returns the full set of available collections as a list.
    request_streams:
        Returns all streams for a given collection, can also be
        used to query for 'active' streams. The streams are returned
        as a list of dictionaries where each dictionary describes a
        stream,
    request_history:
        Queries NNTSC for aggregated historical data for a given 
        time period and set of labels, binned according to the provided
        binsize. Returns a dictionary, keyed by the label.

    All API functions return None in the event of an error.
    """

    def __init__(self, config):
        """
        Init function for the NNTSCConnection class.

        Parameters:
          config -- a dictionary containing the configuration for the NNTSC
                    connection.

        Configuration parameters:
          host: the host that is running the NNTSC you wish to connect to
          port: the port that NNTSC is listening on for clients

          If unspecified, the host defaults to 'localhost' and the port
          defaults to 61234.
        """

        self.client = None
        if 'host' in config:
            self.host = config['host']
        else:
            self.host = "localhost"
        if 'port' in config:
            self.port = int(config['port'])
        else:
            self.port = 61234

    def _connect(self):
        """ 
        Attempts to establish a connection with the NNTSC database.

        Returns None if the connection fails, otherwise it will return
        the newly created NNTSCClient object.
        
        """
        # If we've already got a connection, just use that
        if self.client is not None:
            return self.client
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error, msg:
            log("Failed to create socket: %s" % (msg[1]))
            return None

        attempts = 0
        connected = False

        # XXX Retry forever or die after a certain number of attempts?
        while connected == False:
            if attempts > 0:
                log("Retrying in 30 seconds (attempt %d)" % (attempts + 1))
                time.sleep(30)
        
            try:
                s.connect((self.host, self.port))
                connected = True
            except socket.error, msg:
                log("Failed to connect to %s:%d -- %s" % (
                        self.host, self.port, msg[1]))
                attempts += 1

        if not connected:
            log("Unable to connect to NNTSC after numerous attempts")
            return None

        self.client = NNTSCClient(s)
        return self.client

    def _disconnect(self):
        if self.client:
            self.client.disconnect()
        self.client = None

    def _get_nntsc_message(self):
        """ 
        Waits for NNTSC to send a response to a query. Will block until
        a complete message arrives.

        Returns None if an error occurs, otherwise will return a tuple
        representing the NNTSC message. The first element of the tuple
        is the message type and the second element is a dictionary 
        containing the message contents.
        """
        if self.client == None:
            return None
        
        while 1:
            msg = self.client.parse_message()

            if msg[0] == -1:
                received = self.client.receive_message()
                if received <= 0:
                    log("Failed to receive message from NNTSC")
                    self._disconnect()
                    return None
                continue

            return msg

    def request_collections(self):
        """ 
        Requests a list of collections from the NNTSC database.

        Returns None if the request fails, otherwise will return a list
        of dictionaries where each dictionary describes a collection.
        """
        if self.client == None:
            self._connect()
               
        if self.client == None:
            log("Unable to connect to NNTSC exporter to request collections")
            return None

        self.client.send_request(NNTSC_REQ_COLLECTION, -1)

        msg = self._get_nntsc_message()
        if msg == None:
            self._disconnect()
            return None

        if msg[0] == NNTSC_COLLECTIONS:
            self._disconnect()
            return msg[1]['collections']
        elif msg[0] == NNTSC_QUERY_CANCELLED:
            log("Request for NNTSC Collections timed out")
        else:
            log("Unexpected response to NNTSC Collections request: %d" % (msg[0]))

        self._disconnect()
        return None


    def request_streams(self, colid, reqtype, boundary):
        """ 
        Requests a list of streams from the NNTSC database.
        
        Parameters:
            colid -- The id number of the collection which is being
                     queried
            reqtype -- Should be either NNTSC_REQ_STREAMS for querying all
                       streams or NNTSC_REQ_ACTIVE_STREAMS if only active
                       streams are required
            boundary -- If asking for active streams, this field is a
                        timestamp. Only streams that were last updated since
                        this timestamp will be returned. 
                        Otherwise, this parameter is a stream id. Only 
                        streams that were first observed after this stream
                        was created will be returned.
                        In either case, set this to zero to receive all
                        streams.

        Returns None if the request fails, otherwise returns a list of
        dictionaries where each dictionary represents a single stream.
        """
        streams = []
        
        if self.client == None:
            self._connect()
               
        if self.client == None:
            log("Unable to connect to NNTSC exporter to request streams")
            return None

        self.client.send_request(reqtype, colid, boundary)

        if reqtype == NNTSC_REQ_ACTIVE_STREAMS:
            logreq = "active "
        else:
            logreq = ""

        while 1:
            msg = self._get_nntsc_message()
            if msg == None:
                self._disconnect()
                return None

            # Check if we got a complete parsed message, otherwise read some
            # more data
            if msg[0] == -1:
                continue

            if (msg[0] == NNTSC_STREAMS and reqtype == NNTSC_REQ_STREAMS) or \
                     (msg[0] == NNTSC_ACTIVE_STREAMS and \
                     reqtype == NNTSC_REQ_ACTIVE_STREAMS):
                if msg[1]['collection'] != colid:
                    continue

                streams += msg[1]['streams']
                if msg[1]['more'] == False:
                    break
            elif msg[0] == NNTSC_QUERY_CANCELLED:
                log("Query for %sstreams for collection %d timed out" % (logreq, colid))

                self._disconnect()
                return None
            else:
                log("Received unexpected response to %sstreams request: %d" % (logreq, msg[0]))
                self._disconnect()
                return None

        self._disconnect()
        return streams
        
    def request_history(self, colid, labels, start, end, binsize, aggregators,
            groupcols):
        """ 
        Requests historical time series data from a NNTSC database.

        Parameters:
            colid -- The id number of the collection which is being
                     queried
            labels -- A dictionary describing the streams to be queried.
                      The keys are the label names and the values are a 
                      list of stream ids to be combined to form the time
                      series for that label.
            start -- the start of the time period to query for
            end -- the end of the time period to query for
            binsize -- the bin size (in seconds) to use when aggregating 
                       the time series data, i.e. a binsize of 600 will 
                       produce one data point every 10 minutes. A binsize
                       of zero is a special case that will aggregate the
                       data into a single data point.
            aggregators -- a tuple consisting of two lists. The first list
                           contains the columns to query in the NNTSC
                           database. The second list describes the 
                           aggregation functions that should be applied to
                           each of the columns in the first list. Both
                           lists need to be the same length and the
                           corresponding aggregation function must have the
                           same list index as the column it applies to.
            groupcols -- a list of columns to add to a "GROUP BY" clause
                         in the database query, i.e. columns where only one
                         result should be reported for each unique set of
                         values for those columns.

        Returns None if the request fails, otherwise will return a 
        dictionary keyed by the label name. The dict values are also
        dictionaries containing the following items:
            freq -- the expected time gap between each returned value
            data -- a list containing the aggregated data for the label
            timedout -- a list of tuples describing time periods where
                        the request was not completed due to a query
                        timeout
        """        
                
                
        if self.client == None:
            self._connect()
               
        if self.client == None:
            log("Unable to connect to NNTSC exporter to request historical data")
            return None
        
        result = self.client.request_aggregate(colid, labels, start, end, \
                aggregators[0], binsize, groupcols, aggregators[1])
        if result == -1:
            log("Failed to request aggregate data for collection %d" % (colid))
            self._disconnect()      
            return None

        data = {}
        freq = 0
        count = 0

        while count < len(labels):
            msg = self._get_nntsc_message()
            if msg == None:
                self._disconnect()
                return None

            # Check if we got a complete parsed message, otherwise read some
            # more data
            if msg[0] == -1:
                continue

            # Look out for STREAM packets describing new streams
            if msg[0] == NNTSC_STREAMS:
                continue

            if msg[0] == NNTSC_QUERY_CANCELLED:
                # At least some of the data is missing due to a query timeout
                if msg[1]['collection'] != colid:
                    continue

                for lab in msg[1]['labels']:
                    if lab not in labels:
                        continue
                    if lab not in data:
                        data[lab] = {}
                        data[lab]["data"] = []
                        data[lab]["timedout"] = []

                    data[lab]['timedout'].append((msg[1]['start'], msg[1]['end']))
                    if msg[1]['more'] == False:
                        # Make sure we report some sort of frequency if we
                        # are missing all the data...
                        if "freq" not in data[lab]:
                            data[lab]["freq"] = binsize
                        count += 1
            
            if msg[0] == NNTSC_HISTORY:
                # Sanity checks
                if msg[1]['collection'] != colid:
                    continue
                label = msg[1]['streamid']
                if label not in labels:
                    continue
                if label not in data:
                    data[label] = {}
                    data[label]["data"] = []
                    data[label]["timedout"] = []

                if "freq" not in data[label]:
                    data[label]["freq"] = msg[1]['binsize']
                data[label]["data"] += msg[1]['data']
                if msg[1]['more'] == False:
                    # increment the count of completed labels
                    count += 1
        self._disconnect()
        return data

        
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
