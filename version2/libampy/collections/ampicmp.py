from libnntscclient.logger import *
from libampy.collection import Collection
import re
from operator import itemgetter

class AmpIcmp(Collection):
    def __init__(self, colid, viewmanager, nntscconf):

        super(AmpIcmp, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = ['source', 'destination', 'packet_size', \
                'family']
        self.groupproperties = ['source', 'destination', 'packet_size', \
                'aggregation']
        self.collection_name = "amp-icmp"
        self.splits = {
                "NONE":"by address", 
                "FAMILY":"IPv4/IPv6", 
                "ADDRESS":"to",
                "IPV4":"IPv4",
                "IPV6":"IPv6"}
        self.default_packet_size = "84"
       
    def detail_columns(self, detail):
        # the matrix view expects both the mean and stddev for the latency
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count", "avg", "count"]
            aggcols = ["rtt", "rtt", "rtt", "loss", "loss"]
        elif detail == "basic":
            aggfuncs = ["avg", "avg"]
            aggcols = ["rtt", "loss"]
        else:
            aggfuncs = ["smoke", "avg"]
            aggcols = ["rtt", "loss"] 
    
        return (aggcols, aggfuncs)
        
    def prepare_stream_for_storage(self, stream):
        # Need to add a 'family' property so that we can group streams easily
        if 'address' not in stream:
            return stream, (stream['stream_id'], None)

        if '.' in stream['address']:
            stream['family'] = 'ipv4'
        else:
            stream['family'] = 'ipv6'

        # We need the address for NONE aggregation, so we need to store that
        # along with the stream id

        return stream, {'address':stream['address']}
    
    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s to %s %s" % (groupparams['source'], 
                groupparams['destination'], 
                self.splits[groupparams['aggregation']])

        if 'address' in groupparams:
            label += " %s" % (groupparams['address'])
        return label        

    def _generate_label(self, baselabel, search, family, lookup):
        if family is None:
            key = "baselabel"
            shortlabel = "All addresses"
        else:
            key = baselabel + "_" + family
            search['family'] = family.lower()
            shortlabel = family
        
        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None

            # Just return the list of stream ids, as the addresses don't
            # matter for the aggregation methods that will call this function
            streams = [item[0] for item in streams]
        else:
            streams = []

        return {'labelstring':key, 'streams':streams, 'shortlabel':shortlabel}
            

    def group_to_labels(self, groupid, description, lookup=True):
        labels = []
        
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend lines")
            return None

        baselabel = 'group_%s' % (groupid)
        search = {'source':groupparams['source'], 
                'destination':groupparams['destination'],
                'packet_size':groupparams['packet_size']}

        if groupparams['aggregation'] in ['IPV4', 'FAMILY']:
            nextlab = self._generate_label(baselabel, search, "IPv4", lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        if groupparams['aggregation'] in ['IPV6', 'FAMILY']:
            nextlab = self._generate_label(baselabel, search, "IPv6", lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        if groupparams['aggregation'] == "FULL":
            nextlab = self._generate_label(baselabel, search, None, lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        if groupparams['aggregation'] == "NONE":
            # Unfortunately, we have to do a lookup regardless here to 
            # determine suitable short labels for each line
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for %s, %s" % \
                        (baselabel, self.collection_name))
                return None

            for sid, store in streams:
                if 'address' not in store:
                    log("Error: no address stored with stream id %s" % (sid))
                    return None
                address = store['address']
                nextlab = {'labelstring':baselabel + "_" + address,
                        'streams':[sid], 'shortlabel':address}
                labels.append(nextlab)

        return sorted(labels, key=itemgetter('shortlabel'))

    def create_group_description(self, properties):

        # If we're creating a description based on an existing group or
        # stream, we need to convert the 'family' into an appropriate
        # aggregation method.
        if 'family' in properties:
            properties['aggregation'] = properties['family'].upper()

        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                    (p, self.collection_name))
                return None
        
        return "FROM %s TO %s OPTION %s %s" % ( \
                properties['source'], properties['destination'],
                properties['packet_size'], properties['aggregation'].upper())

    def parse_group_description(self, description):
        parts = re.match("FROM (?P<source>[.a-zA-Z0-9-]+) "
                "TO (?P<destination>[.a-zA-Z0-9-]+) "
                "OPTION (?P<option>[a-zA-Z0-9]+) "
                "(?P<split>[A-Z0-9]+)[ ]*(?P<address>[0-9.:a-zA-Z]*)",
                description)

        if parts is None:
            log("Group description did not match regex for amp-icmp")
            log(description)
            return None
        if parts.group("split") not in self.splits:
            log("amp-icmp group description has no aggregation method")
            log(description)
            return None

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "packet_size": parts.group("option"),
            "aggregation": parts.group("split")
        }

        if parts.group("address") is not None:
            keydict["address"] = parts.group("address")

        return keydict
    
    def update_matrix_groups(self, source, dest, options, groups):
        
        if len(options) < 1:
            log("Missing packet size option when building matrix groups")
            return groups

        groupprops = {
            'source':source, 'destination':dest, 'packet_size':options[0]
        }

        self._matrix_group_streams(groupprops, 'ipv4', groups)
        self._matrix_group_streams(groupprops, 'ipv6', groups)

    def translate_group(self, groupprops):
        defaultsize = self.default_packet_size

        if 'source' not in groupprops:
            return None
        if 'destination' not in groupprops:
            return None

        if 'packet_size' not in groupprops:
            packetsize = defaultsize
        else:
            packetsize = groupprops['packet_size']

        newprops = {'source':groupprops['source'],
                'destination':groupprops['destination']}

        req, sizes = self.streammanager.find_selections(newprops)

        if req != 'packet_size':
            log("Unable to find packet sizes for %s %s to %s" % \
                    (self.collection_name, newprops['source'], \
                    newprops['destination']))
            return None

        if sizes == []:
            packetsize = defaultsize
        elif packetsize not in sizes:
            if defaultsize in sizes:
                packetsize = defaultsize
            else:
                sizes.sort(key=int)
                packetsize = sizes[0]

        if 'aggregation' not in groupprops:
            agg = "FAMILY"
        else:
            agg = groupprops['aggregation']

        newprops['aggregation'] = agg
        newprops['packet_size'] = packetsize

        return self.create_group_description(newprops)

    def _matrix_group_streams(self, baseprops, family, groups):
        
        baseprops['family'] = family
        label = "%s_%s_%s" % (baseprops['source'], baseprops['destination'], 
                family)
        streams = self.streammanager.find_streams(baseprops)

        if len(streams) > 0:
            # Remove the addresses, we just need the stream ids
            streams = [item[0] for item in streams]
            groups.append({'labelstring':label, 'streams':streams})

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :

