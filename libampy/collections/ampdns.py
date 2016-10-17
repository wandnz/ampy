from libnntscclient.logger import *
from libampy.collection import Collection
from operator import itemgetter

class AmpDns(Collection):

    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpDns, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = ['source', 'destination', 'recurse',
                'query', 'query_type', 'query_class', 'udp_payload_size',
                'dnssec', 'nsid']
        self.groupproperties = ['source', 'destination', 'query',
                'query_type', 'query_class', 'udp_payload_size',
                'flags', 'aggregation']
        self.integerproperties = ['udp_payload_size']
        self.collection_name = "amp-dns"
        self.viewstyle = "amp-latency"
        self.defaults = {
            "query": "google.com",
            "query_type": "A",
            "query_class": "IN",
            "udp_payload_size": 4096,
            "nsid": False,
            "dnssec": False,
        }

    def detail_columns(self, detail):
        if detail == "matrix" or detail == "basic":
            aggfuncs = ["avg", "stddev", "count", "count"]
            aggcols = ["rtt", "rtt", "rtt", "timestamp"]
        elif detail == "full" or detail == "raw" or detail == "summary":
            aggfuncs = ["smoke", "count", "count"]
            aggcols = ["rtt", "rtt", "timestamp"]
        else:
            aggfuncs = ["avg"]
            aggcols = ["rtt"]

        return aggcols, aggfuncs

    def calculate_binsize(self, start, end, detail):
        if (end - start) / 60.0 < 200:
            return 60

        return super(AmpDns, self).calculate_binsize(start, end, detail)

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
        elif groupparams['aggregation'] == "FAMILY":
            agg = "IPv4/IPv6"
        elif groupparams['aggregation'] == "IPV4":
            agg = "IPv4"
        elif groupparams['aggregation'] == "IPV6":
            agg = "IPv6"
        else:
            agg = ""

        label = "%s to %s DNS, %s %s %s %s %s" % ( \
                groupparams['source'], groupparams['destination'],
                groupparams['query'], groupparams['query_class'],
                groupparams['query_type'], groupparams['udp_payload_size'],
                flags)
        return label, agg

    def _lookup_streams(self, search, lookup, baselabel):
        streams = []

        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log ("Failed to find streams for label %s, %s" % \
                        (baselabel, self.collection_name))
                return None

        return streams

    def _generate_family_label(self, baselabel, search, family, lookup):
        key = baselabel + "_" + family
        shortlabel = family

        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None

            famstreams = []
            for sid, store in streams:
                if 'address' not in store:
                    continue
                if family.lower() == self._address_to_family(store['address']):
                    famstreams.append(sid)
        else:
            famstreams = []

        return {'labelstring':key, 'streams':famstreams,
                'shortlabel':shortlabel}

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
            streams = self._lookup_streams(search, lookup, baselabel)
            if streams is None:
                return None

            # Discard the addresses stored with each stream
            streams = [item[0] for item in streams]
            lab = {'labelstring':baselabel, 'streams':streams,
                    'shortlabel':'All instances'}
            labels.append(lab)
        elif groupparams['aggregation'] in ["FAMILY", "IPV4", "IPV6"]:
            if groupparams['aggregation'] != "IPV6":
                nextlab = self._generate_family_label(baselabel, search,
                    "IPv4", lookup)
                if nextlab is None:
                    return None
                labels.append(nextlab)
            if groupparams['aggregation'] != "IPV4":
                nextlab = self._generate_family_label(baselabel, search,
                    "IPv6", lookup)
                if nextlab is None:
                    return None
                labels.append(nextlab)
        else:
            streams = self._lookup_streams(search, True, baselabel)
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
            properties['aggregation'] = "FAMILY"

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
        regex = "FROM (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9-:]+) "
        regex += "OPTION (?P<query>[a-zA-Z0-9.]+) (?P<type>[A-Z]+) "
        regex += "(?P<class>[A-Z]+) "
        regex += "(?P<size>[0-9]+) (?P<flags>[TF]+) "
        regex += "(?P<split>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group("split") not in ['FULL', 'NONE', 'FAMILY', 'IPV4',
                    'IPV6']:
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

    def update_matrix_groups(self, source, dest, split, groups, views,
            viewmanager, viewstyle):

        # Firstly, we want to try to populate our matrix cell using streams
        # where the target DNS server is the authoritative server, if
        # any such streams are available. This can be done by looking for
        # streams where there was no recursion (and assuming each
        # authoritative server is only hit with one query!).
        #
        # If no such streams are available, the server is probably a
        # public DNS server. In this case, we're going to try to use
        # streams for resolving google.com IN A with the default settings.
        # This should be cached, so we should get a reasonable estimate of
        # server performance. If for some reason there isn't a stream that
        # matches this criteria, then pick an arbitrary set of properties
        # in order to get *something* to display.
        #
        # Note, this assumes that we are always going to test www.google.com
        # for each non-authoritative server but surely we can manage
        # to do this.
        groupprops = {
            'source':source, 'destination':dest, 'recurse':False
        }

        # TODO should check if there are missing properties that prevented
        # us from finding a stream without recursion
        streams = self.streammanager.find_streams(groupprops)

        if len(streams) == 0:
            # no streams without recursion, try with recursion
            groupprops['recurse'] = True
            props = self.get_selections(groupprops, "", "1", 30000, False)

            if props is None:
                return

            # as long as there are properties that need setting, keep setting
            # them until we get a stream id (that hopefully has recent data!)
            while len(props) > 0:
                for prop,values in props.iteritems():
                    # prefer the default values, but if they aren't present
                    # then pick the first option available
                    if prop in self.defaults and \
                                any(n['text'] == self.defaults[prop] \
                                    for n in values['items']):
                        groupprops[prop] = self.defaults[prop]
                    else:
                        groupprops[prop] = values['items'][0]['text']
                props = self.get_selections(groupprops, "", "1", 30000, False)
            streams = self.streammanager.find_streams(groupprops)

        v4streams = []
        v6streams = []

        cellgroups = set()

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

            streamprops = self.streammanager.find_stream_properties(sid)
            if split == "ipv4":
                streamprops['aggregation'] = "IPV4"
            elif split == "ipv6":
                streamprops['aggregation'] = "IPV6"
            else:
                streamprops['aggregation'] = "FAMILY"

            groupdesc = self.create_group_description(streamprops)
            cellgroups.add(groupdesc)

        if len(cellgroups) != 0:
            cellview = viewmanager.add_groups_to_view(viewstyle,
                    self.collection_name, 0, list(cellgroups))
        else:
            cellview = -1

        if cellview is None:
            views[(source,dest)] = -1
        else:
            views[(source,dest)] = cellview

        # Add the two new groups
        if len(v4streams) > 0:
            groups.append({
                'labelstring':'%s_%s_ipv4' % (source, dest),
                'streams':v4streams
            })

        if len(v6streams) > 0:
            groups.append({
                'labelstring':'%s_%s_ipv6' % (source, dest),
                'streams':v6streams
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
