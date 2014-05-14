import operator

from libampy.ampmesh import AmpMesh
from libampy.viewmanager import ViewManager
from libampy.collection import Collection
from libampy.nntsc import NNTSCConnection
from libampy.cache import AmpyCache
from libampy.eventmanager import EventManager

from libnntscclient.logger import *

from libampy.collections.ampicmp import AmpIcmp
from libampy.collections.amptraceroute import AmpTraceroute

class Ampy(object):

    def __init__(self, ampdbconf, viewconf, nntscconf, eventconf):
        self.ampmesh = AmpMesh(ampdbconf)
        self.viewmanager = ViewManager(viewconf)
        self.nntscconfig = nntscconf
        self.cache = AmpyCache(12)
        self.eventmanager = EventManager(eventconf)

        self.collections = {}
        self.savedcoldata = {}

    def start(self):
        return self._query_collections()

    def get_collections(self):
        return self.savedcoldata.keys()

    def get_recent_data(self, collection, view_id, duration, detail):
        alllabels = []

        col, viewgroups = self._view_to_groups(collection, view_id)
        if col is None:
            log("Failed to fetch recent data")
            return None

        for gid, descr in viewgroups.iteritems():
            grouplabels = col.group_to_labels(gid, descr, True)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue
            alllabels += grouplabels

        return self._fetch_recent(col, alllabels, duration, detail)


    def get_historic_data(self, collection, view_id, start, end, binsize, \
            detail):

        alllabels = []

        col, viewgroups = self._view_to_groups(collection, view_id)
        if col is None:
            log("Failed to fetch historic data")
            return None

        # Break the time period down into blocks for caching purposes
        blocks = self.cache.get_caching_blocks(start, end, binsize, 2)

        # Find all labels for this view and their corresponding streams
        for gid, descr in viewgroups.iteritems():
            grouplabels = col.group_to_labels(gid, descr, True)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue
            alllabels += grouplabels

        # Figure out which blocks are cached and which need to be queried 
        notcached, cached = self._find_cached_data(col, blocks, alllabels, 
                start, end, binsize, detail)

        # Fetch all uncached data
        fetched = frequencies = timeouts = {}
        if len(notcached) != 0:
            fetch = self._fetch_uncached_data(col, notcached, binsize, detail)
            if fetch is None:
                return None

            fetched, frequencies, timeouts = fetch

        # Merge fetched data with cached data to produce complete series

        data = {}
        for label, dbdata in fetched.iteritems():
            data[label] = []
            failed = timeouts[label]

            for b in blocks:
                blockdata, dbdata = self._next_block(col, b, cached[label], dbdata, 
                    frequencies[label], binsize)
                
                data[label] += blockdata

                # Store this block in our cache for fast lookup next time
                # If it already is there, we'll reset the cache timeout instead
                failed = self.cache.store_block(b, blockdata, label, binsize, 
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
                blockdata, ignored = self._next_block(col, b, cached[label], [], 
                        0, binsize)
                data[label] += blockdata
                ignored = self.cache.store_block(b, blockdata, label, binsize, detail, [])


        return data

    def get_view_legend(self, collection, view_id):
        col, viewgroups = self._view_to_groups(collection, view_id)
        if col is None:
            log("Failed to generate legend")
            return None

        legend = []

        # What we need:
        # A set of 'legend' entries, one per group
        # For each entry, we also need a set of 'lines', one per group member

        nextlineid = 0
        
        # Sort the groups in the view by description.
        # This ensures that A) the legend will be in a consistent order
        # and B) the ordering is more obvious to the user (i.e. alphabetical
        # starting with the first group property)
        viewgroups = sorted(viewgroups.iteritems(), key=operator.itemgetter(1))
        for gid, descr in viewgroups:
            legendtext = col.get_legend_label(descr)
            if legendtext is None:
                legendtext = "Unknown"

            # Don't lookup the streams themselves if we can avoid it
            grouplabels = col.group_to_labels(gid, descr, False)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue
            lines = []

            # Yes, we could assign line ids within group_to_labels but
            # then anyone implementing a collection has to make sure they
            # remember to do it. Also these ids are only needed for legends,
            # but group_to_labels is also used for other purposes so it
            # is cleaner to do it here even if it means an extra iteration 
            # over the grouplabels list.
            for gl in grouplabels:
                lines.append((gl['labelstring'], gl['shortlabel'], nextlineid))
                nextlineid += 1

            legend.append({'group_id':gid, 'label':legendtext, 'lines':lines})

        return legend
        

    def get_selection_options(self, collection, selected):

        col = self._getcol(collection)
        if col == None:
            log("Error while fetching selection options")
            return None

        if col.update_streams() is None:
            log("Error while fetching selection options")
            return None

        options = col.get_selections(selected)
        return options       

    def test_graphtab_view(self, collection, tabcollection, view_id):
           
        col, groups = self._view_to_groups(collection, view_id)   
        if col == None:
            log("Error while constructing tabview")
            return None
        
        tabcol = self._getcol(tabcollection)
        if tabcol == None:
            log("Error while constructing tabview")
            return None
        if tabcol.update_streams() is None:
            log("Error while fetching selection options for tab collection")
            return None

        for gid, descr in groups.iteritems():
            grouprule = col.parse_group_description(descr)

            tabrule = tabcol.translate_group(grouprule)
            print tabrule
            if tabrule is None:
                continue

            labels = tabcol.group_to_labels('tabcheck', tabrule, True)
            for lab in labels:
                # We can bail as soon as we get one group with a stream
                if len(lab['streams']) > 0:
                    return True
        
        # If we get here, none of the translated groups would match any
        # streams in the database
        return False

    def create_graphtab_view(self, collection, tabcollection, view_id):
        col, groups = self._view_to_groups(collection, view_id)   
        if col == None:
            log("Error while constructing tabview")
            return None
        
        tabcol = self._getcol(tabcollection)
        if tabcol == None:
            log("Error while constructing tabview")
            return None
        if tabcol.update_streams() is None:
            log("Error while fetching selection options for tab collection")
            return None

        tabgroups = []
        for gid, descr in groups.iteritems():
            grouprule = col.parse_group_description(descr)

            tabrule = tabcol.translate_group(grouprule)
            if tabrule is None:
                continue

            tabid = self.viewmanager.get_group_id(tabcollection, tabrule)
            if tabid is None:
                continue

            tabgroups.append(tabcol)

        if len(tabgroups) == 0:
            log("Unable to create tabview %s to %s for view %s" % \
                    (collection, tabcollection, view_id))
            log("No valid groups were found for new tab")
            return None

        tabgroups.sort()
        tabview = self.viewmanager.get_view_id(tabcollection, tabgroups)
        if tabview is None:
            log("Unable to create tabview %s to %s for view %s" % \
                    (collection, tabcollection, view_id))
            log("Could not create new view")
            return None

        return tabview

    def get_event_view(self, collection, stream):
        col = self._getcol(collection)
        if col == None:
            log("Error while creating event view")
            return None
        
        if col.update_streams() is None:
            log("Error while creating event view")
            return None
        
        streamprops = col.find_stream(stream)
        if streamprops is None:
            log("Error while creating event view")
            log("Stream %s does not exist for collection %s" % \
                    (stream, collection))
            return None

        eventgroup = col.create_group_description(streamprops)
        if eventgroup is None:
            log("Error while creating event view")
            log("Unable to generate group for stream id %s (%s)" % \
                    (stream, collection))
            return None

        return self.viewmanager.add_group_to_view(collection, 0, eventgroup)

    def modify_view(self, collection, view_id, action, options):
        col = self._getcol(collection)
        if col == None:
            return None
        if len(options) == 0:
            return view_id


        if action == "add":
            newgroup = col.create_group_from_list(options)
            if newgroup is None:
                return view_id
            return self.viewmanager.add_group_to_view(collection, view_id, 
                    newgroup)
        elif action == "del":
            groupid = int(options[0])
            return self.viewmanager.remove_group_from_view(collection, 
                    view_id, groupid)
        else:
            return view_id

    def get_matrix_data(self, collection, options, duration):
        col = self._getcol(collection)
        if col == None:
            log("Error while fetching matrix data")
            return None
        
        matrixgroups = self._get_matrix_groups(col, options)
        if matrixgroups is None:
            return None

        return self._fetch_recent(col, matrixgroups, duration, "matrix")   

    def get_view_events(self, collection, view_id, start, end):
        col, groups = self._view_to_groups(collection, view_id)   
        if col == None:
            log("Error while fetching events for a view")
            return None
      
        alllabels = [] 
        for gid, descr in groups.iteritems():
            grouplabels = col.group_to_labels(gid, descr, True)
            if grouplabels is None:
                log("Unable to convert group %d into stream labels" % (gid))
                continue

            alllabels += grouplabels

        return self.eventmanager.fetch_events(alllabels, start, end)

    def get_event_groups(self, start, end):
        return self.eventmanager.fetch_groups(start, end)

    def get_event_group_members(self, eventgroupid):
        return self.eventmanager.fetch_event_group_members(eventgroupid)


    def _fetch_recent(self, col, alllabels, duration, detail):
        recent = {}
        timeouts = {}
        uncached = {}
        querylabels = {}
        end = int(time.time())
        start = end - duration
       
        for lab in alllabels:
            # Check if we have recent data cached for this label
            cachehit = self.cache.search_recent(lab['labelstring'], 
                    duration, detail)
            # Got cached data, add it directly to our result
            if cachehit is not None:
                recent[lab['labelstring']] = cachehit
                continue

            # Not cached, need to fetch it
            # Limit our query to active streams
            lab['streams'] = col.filter_active_streams(lab['streams'], 
                    start, end)
   
            # If no streams were active, don't query for them. Instead
            # add an empty list to the result for this label.
            if len(lab['streams']) == 0:
                recent[lab['labelstring']] = []
            else:
                querylabels[lab['labelstring']] = lab['streams']

        if len(querylabels) > 0:
            # Fetch data for labels that weren't cached using one big
            # query
            result = col.fetch_history(querylabels, start, end, duration, 
                    detail)

            for label, queryresult in result.iteritems():
                formatted = col.format_list_data(queryresult['data'], queryresult['freq']) 
                # Cache the result
                self.cache.store_recent(label, duration, detail, formatted)

                # Add the result to our return dictionary
                recent[label] = formatted
                
                # Also update the timeouts dictionary
                if len(queryresult['timedout']) != 0:
                    timeouts.append(label)

        return recent, timeouts

    def _next_block(self, col, block, cached, queried, freq, binsize):
        
        # If this block is cached, we can return the cached data right away
        if block['start'] in cached:
            return cached[block['start']], queried
        
        if freq > binsize:
            # Measurements do not align nicely with our request binsize so
            # be very careful about how we match query results to blocks
            incrementby = freq
            usekey = 'timestamp'

            # Measurements don't always happen exactly on the frequency,
            # i.e. there can be a second or two of delay. Ideally, we
            # should account for this when we are searching for the next
            # data point
            delayfactor = 10 
            
        else:
            incrementby = binsize
            usekey = 'binstart'

            # The database will always give us nice round timestamps
            # based on the requested binsize
            delayfactor = 0
   
        blockdata = []
        ts = block['start']

        while ts < block['end']:
            # We are unlikely to be predicting the future
            if ts > time.time():
                break

            if len(queried) == 0:
                # No more queried data so we must be missing a measurement
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
                    datum = col.format_single_data(queried[0], freq)
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


    
    def _find_cached_data(self, col, blocks, labels, start, end, binsize, 
            detail):
        notcached = {}
        cached = {}
        for label in labels:
            # Check which blocks are cached and which are not
            missing, found = self.cache.search_cached_blocks(blocks, start,
                    end, binsize, detail, label['labelstring'])
            
            cached[label['labelstring']] = found
            
            # This skips the active stream filtering if the entire label is
            # already cached
            if len(missing) == 0:
                continue
            
            # Remove inactive streams from the streams list
            label['streams'] = col.filter_active_streams(label['streams'], 
                    start, end)

            # Add missing blocks to the list of data to be fetched from NNTSC
            for b in missing:        
                if b not in notcached:
                    notcached[b] = {label['labelstring']: label['streams']}
                else:
                    notcached[b][label['labelstring']] = label['streams']

        return notcached, cached

    def _fetch_uncached_data(self, col, notcached, binsize, detail):

        fetched = {}
        frequencies = {}
        timeouts = {}

        # Query NNTSC for all of the missing blocks
        for (bstart, bend), labels in notcached.iteritems():
            hist = col.fetch_history(labels, bstart, bend-1, binsize, detail)
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

    
    def _query_collections(self):
        nntscdb = NNTSCConnection(self.nntscconfig)

        collections = nntscdb.request_collections()
        if collections is None:
            log("Failed to query NNTSC for existing collections")
            return None

        for col in collections:
            name = col['module'] + "-" + col['modsubtype']
            self.savedcoldata[name] = col['id']

        return len(self.savedcoldata.keys())

    def _getcol(self, collection):
        newcol = None
        # If we have a matching collection, return that otherwise create a
        # new instance of the collection

        if collection in self.collections:
            return self.collections[collection]

        if collection not in self.savedcoldata:
            log("Collection type %s does not exist in NNTSC database" % \
                    (collection))
            return None
   
        colid = self.savedcoldata[collection] 
        if collection == "amp-icmp":
            newcol = AmpIcmp(colid, self.viewmanager, self.nntscconfig)
        if collection == "amp-traceroute":
            newcol = AmpTraceroute(colid, self.viewmanager, self.nntscconfig)

        if newcol is None:
            log("Unknown collection type: %s" % (collection))
            return None

        self.collections[collection] = newcol
        return newcol

    def _view_to_groups(self, collection, view_id):
        col = self._getcol(collection)
        if col == None:
            return None, None
        
        if col.update_streams() is None:
            return None, None
        
        viewgroups = self.viewmanager.get_view_groups(collection, view_id)

        if viewgroups is None:
            log("Unable to find groups for view id %d(%s)" % (view_id, collection))
            return None, None
        
        return col, viewgroups

    def _get_matrix_groups(self, col, options):
        if len(options) < 2:
            log("Invalid options for fetching matrix streams")
            return None

        if col.update_streams() is None:
            log("Error while fetching matrix streams")
            return None
        
        sourcemesh = options[0]
        destmesh = options[1]

        sources = self.ampmesh.get_sources(sourcemesh)
        if sources is None:
            log("Error while fetching matrix streams")
            return None

        destinations = self.ampmesh.get_destinations(destmesh)
        if destinations is None:
            log("Error while fetching matrix streams")
            return None

        groups = []

        for s in sources:
            for d in destinations:
                col.update_matrix_groups(s, d, options[2:], groups)

        return groups


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
