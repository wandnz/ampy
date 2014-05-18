from libnntscclient.logger import *
from libampy.collection import Collection
import re
from operator import itemgetter

class AmpDns(Collection):

    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpDns, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = ['source', 'destination', 
                'query', 'query_class', 'query_type', 'udp_payload_size',
                'recurse', 'dnssec', 'nsid']
        self.groupproperties = ['source', 'destination', 'query', 
                'query_class', 'query_type', 'udp_payload_size',
                'flags', 'aggregation']
        self.collection_name = "amp-dns"

    
    def detail_columns(self, detail):
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count"]
            aggcols = ["rtt", "rtt", "rtt"]
        elif detail == "full":
            aggfuncs = ["smoke"]
            aggcols = ["rtt"]
        else:
            aggfuncs = ["avg"]
            aggcols = ["rtt"]

        return aggcols, aggfuncs

    def prepare_stream_for_storage(self, stream):
        if 'address' not in stream:
            return stream, {}
        return stream, {'address':stream['address']}

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        flags = ""
        if groupparams["flags"][0] == "T":
            flags += "+recurse "
        if groupparams["flags"][1] == "T":
            flags += "+dnssec "
        if groupparams["flags"][2] == "T":
            flags += "+nsid "

        if groupparams['aggregation'] == "FULL":
            agg = "combined instances"
        else:
            agg = ""

        label = "%s to %s, %s %s %s %s %s %s" % ( \
                groupparams['source'], groupparams['destination'],
                groupparams['query'], groupparams['query_class'],
                groupparams['query_type'], groupparams['udp_payload_size'],
                flags, agg)
        return label
   
    def _lookup_streams(self, search, lookup):
        streams = []
    
        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log ("Failed to find streams for label %s, %s" % \
                        (baselabel, self.collection_name))
                return None

        return streams

    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate labels")
            return None

        baselabel = 'group_%s' % (groupid)
        search = {'source':groupparams['source'],
                'destination':groupparams['destination'],
                'query':groupparams['query'],
                'query_type':groupparams['query_type'],
                'query_class':groupparams['query_class'],
                'udp_payload_size':int(groupparams['udp_payload_size']),
                'recurse':False,
                'dnssec':False,
                'nsid':False,
        }

        if groupparams["flags"][0] == "T":
            search['recurse'] = True
        if groupparams["flags"][1] == "T":
            search['dnssec'] = True
        if groupparams["flags"][2] == "T":
            search['nsid'] = True
       

        if groupparams['aggregation'] == "FULL":
            streams = self._lookup_streams(search, lookup)
            if streams is None:
                return None
            
            # Discard the addresses stored with each stream
            streams = [item[0] for item in streams]
            lab = {'labelstring':baselabel, 'streams':streams, 
                    'shortlabel':'All instances'}
            labels.append(lab)
        else:
            streams = self._lookup_streams(search, True)
            if streams is None:
                return None

            for sid, store in streams:
                if 'address' not in store:
                    log("Error: no address stored with stream id %s" % (sid))
                    return None
                address = store['address']
                nextlab = {'labelstring':baselabel + "_" + address,
                        'streams':[sid], 
                        'shortlabel':'%s (%s)' % (groupparams['destination'], \
                                address)}
                labels.append(nextlab)

        return sorted(labels, key=itemgetter('shortlabel'))

    def create_group_description(self, properties):
        
        # Put in a suitable aggregation method if one is not present, i.e.
        # we are converting a stream into a group
        if 'aggregation' not in properties:
            properties['aggregation'] = "NONE"

        # Convert flags into the flag string
        if 'flags' not in properties:
            properties['flags'] = self._create_flag_string(properties)

        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        return "FROM %s TO %s OPTION %s %s %s %s %s %s" % \
                tuple([properties[x] for x in self.groupproperties])
        
           
    def parse_group_description(self, description):
        parts = re.match("FROM (?P<source>[.a-zA-Z0-9-]+) "
                "TO (?P<destination>[.a-zA-Z0-9-:]+) "
                "OPTION (?P<query>[a-zA-Z0-9.]+) (?P<class>[A-Z]+) "
                "(?P<type>[A-Z]+) "
                "(?P<size>[0-9]+) (?P<flags>[TF]+) "
                "(?P<split>[A-Z]+)",
                description)

        if parts is None:
            log("Group description did not match regex for %s" % \
                    (self.collection_name))
            log(description)
            return None

        if parts.group("split") not in ['FULL', 'NONE']:
            log("%s group description has no aggregation method" % \
                    (self.collection_name))
            log(description)
            return None

        keydict = {
            'source': parts.group("source"),
            'destination': parts.group("destination"),
            'query': parts.group("query"),
            'query_class': parts.group("class"),
            'query_type': parts.group("type"),
            'udp_payload_size': parts.group("size"),
            'flags': parts.group("flags"),
            'aggregation': parts.group("split"),
        }

        return keydict

    def update_matrix_groups(self, source, dest, options, groups):
    
        # TODO Evaluate which options we don't care about in the matrix,
        # i.e. do we care if we combine all the flag combinations into a
        # single value?

        if len(options) < 5:
            log("Not all options present when building matrix groups for %s" \
                    % (self.collection_name))
            return groups

        groupprops = {
            'source':source, 'destination':dest, 'query':options[0],
            'query_class':options[1], 'query_type':options[2],
            'udp_payload_size':int(options[3])
        }

        v4streams = []
        v6streams = []

        streams = self.streammanager.find_streams(groupprops)
        if source == 'prophet':
            print dest, streams
        # Split the resulting streams into v4 and v6 groups based on the
        # stored address
        for sid, store in streams:
            if 'address' not in store:
                continue
            address = store['address']
            if self._address_to_family(address) == 'ipv4':
                v4streams.append(sid)
            else:
                v6streams.append(sid)

        # Add the two new groups
        groups.append({
            'labelstring':'%s_%s_ipv4' % (source, dest),
            'streams':v4streams
        })

        groups.append({
            'labelstring':'%s_%s_ipv6' % (source, dest),
            'streams':v4streams
        })

            
    def _create_flag_string(self, properties):

        flags = ""
        if 'recurse' in properties and properties['recurse'] == True:
            flags += "T"
        else:
            flags += "F"

        if 'dnssec' in properties and properties['dnssec'] == True:
            flags += "T"
        else:
            flags += "F"

        if 'nsid' in properties and properties['nsid'] == True:
            flags += "T"
        else:
            flags += "F"

        return flags
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
