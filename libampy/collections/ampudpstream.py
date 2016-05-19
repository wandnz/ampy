from libnntscclient.logger import *
from libampy.collections.ampthroughput import AmpThroughput
from operator import itemgetter

class AmpUdpstream(AmpThroughput):

    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpUdpstream, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'destination', 'dscp',
                'packet_size', 'packet_spacing', 'packet_count', 'direction',
                'family']
        self.groupproperties = self.streamproperties
        self.integerproperties = ['packet_size', 'packet_spacing',
                'packet_count']
        self.collection_name = "amp-udpstream"
        self.viewstyle = "amp-udpstream"

        self.default_size = 100
        self.default_spacing = 20000
        self.default_count = 101
        self.default_dscp = "Default"

        self.dirlabels = {"in": "Inward", "out": "Outward"}

    def extra_blocks(self, detail):
        if detail in ["jitter", "full"]:
            return 2
        return 0;

    def detail_columns(self, detail):
        if detail == "jitter" or detail == "jitter-summary" or detail == "raw":
            aggcols = ["min_jitter", "jitter_percentile_10",
                    "jitter_percentile_20",
                    "jitter_percentile_30",
                    "jitter_percentile_40",
                    "jitter_percentile_50",
                    "jitter_percentile_60",
                    "jitter_percentile_70",
                    "jitter_percentile_80",
                    "jitter_percentile_90",
                    "jitter_percentile_100"
            ]
            aggmethods = ['mean'] * len(aggcols)
            return (aggcols, aggmethods)

        if detail == "matrix":
            aggcols = ['packets_sent', 'packets_recvd', 'mean_rtt', 'mean_rtt',
                    'mean_rtt']
            aggmethods = ['sum', 'sum', 'avg', 'stddev', 'count']
            return (aggcols, aggmethods)

        if detail == "basic":
            aggcols = ['packets_sent', 'packets_recvd', 'mean_rtt']
            aggmethods = ['sum', 'sum', 'avg']
            return (aggcols, aggmethods)

        return ( \
            ["mean_jitter", "mean_rtt", "packets_recvd", "packets_sent"],
            ["mean", "mean", "sum", "sum"],
        )

    def calculate_binsize(self, start, end, detail):
        minbin = int(((end - start)) / 200)

        if minbin <= 600:
            binsize = 600
        elif minbin <= 1200:
            binsize = 1200
        elif minbin <= 2400:
            binsize = 2400
        elif minbin <= 4800:
            binsize = 4800
        else:
            binsize = 14400

        return binsize


    def create_group_description(self, properties):
        if 'direction' not in properties:
            properties['direction'] = "BOTH"
        if 'family' not in properties and 'address' in properties:
            properties['family'] = \
                    self._address_to_family(properties['address'])

        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        properties['direction'] = properties['direction'].upper()
        properties['family'] = properties['family'].upper()

        return "FROM %s TO %s DSCP %s SIZE %s SPACING %s COUNT %s DIRECTION %s %s" \
                % (properties['source'], properties['destination'],
                   properties['dscp'], properties['packet_size'],
                   properties['packet_spacing'], properties['packet_count'],
                   properties['direction'], properties['family'])


    def get_legend_label(self, description):
        gps = self.parse_group_description(description)
        if gps is None:
            log("Failed to parse group description to generate %s legend label" % (self.collection_name))
            return None, ""

        if gps['family'] == "IPV4":
            family = "IPv4"
        elif gps['family'] == "IPV6":
            family = "IPv6"
        elif gps['family'] == "FAMILY":
            family = "IPv4/IPv6"
        else:
            family = ""

        if gps['direction'] == "BOTH":
            dirstr = ""
        elif gps['direction'] == "IN":
            dirstr = " Inward"
        else:
            dirstr = " Outward"

        label = "%s : %s, %s %sB pkts, %s usec apart (DSCP %s)" % \
                (gps['source'], \
                gps['destination'], gps['packet_count'], gps['packet_size'], \
                gps['packet_spacing'], gps['dscp'])
        return label, "%s%s" % (family, dirstr)


    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9-:]+) "
        regex += "DSCP (?P<dscp>[a-zA-Z0-9-]+) "
        regex += "SIZE (?P<size>[0-9-]+) "
        regex += "SPACING (?P<spacing>[0-9-]+) "
        regex += "COUNT (?P<count>[0-9-]+) "
        regex += "DIRECTION (?P<direction>[A-Z]+) "
        regex += "(?P<family>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('direction') not in ['IN', 'OUT', 'BOTH']:
            log("%s is not a valid direction for a %s group" % \
                    (parts.group('direction'), self.collection_name))
            return None

        if parts.group('family') not in ['IPV4', 'IPV6', 'FAMILY', 'NONE']:
            log("%s is not a valid address family for a %s group" % \
                    (parts.group('family'), self.collection_name))
            return None

        keydict = {
            'source': parts.group('source'),
            'destination': parts.group("destination"),
            'family': parts.group("family"),
            'direction': parts.group("direction"),
            'packet_size': int(parts.group('size')),
            'packet_count': int(parts.group('count')),
            'packet_spacing': int(parts.group('spacing')),
            'dscp': parts.group('dscp'),
        }

        return keydict

    def update_matrix_groups(self, source, dest, split, groups, views,
            viewmanager, viewstyle):

        baseprop = {'source': source, 'destination': dest,
                'dscp': self.default_dscp, 'packet_size': self.default_size,
                'packet_spacing': self.default_spacing }
        sels = self.streammanager.find_selections(baseprop, False)
        if sels is None:
            return None

        req, counts = sels
        if req != "packet_count":
            log("Unable to find suitable packet counters for %s matrix cell %s to %s" \
                    (self.collection_name, source, dest))
            return None

        if counts == []:
            views[(source, dest)] = -1
            return
        
        baseprop['packet_count'] = max(counts);

        v4 = self._matrix_group_streams(baseprop, "out", "ipv4", groups);
        v6 = self._matrix_group_streams(baseprop, "out", "ipv6", groups);

        if v4 == 0 and v6 == 0:
            views[(source, dest)] = -1
            return

        if split == "ipv4":
            split = "IPV4"
        elif split == "ipv6":
            split = "IPV6"
        else:
            split = "FAMILY"


        v = self._add_matrix_group(baseprop, "OUT", split, viewmanager,
                viewstyle)
        views[(source, dest)] = v

        return
    
    def _matrix_group_streams(self, baseprops, direction, family, groups):

        baseprops['direction'] = direction
        baseprops['family'] = family
        label = "%s_%s_%s_%s" % (baseprops['source'], baseprops['destination'],
                direction, family)
        streams = self.streammanager.find_streams(baseprops)

        if len(streams) > 0:
            groups.append({'labelstring': label,
                    'streams': [x[0] for x in streams]})
        return len(streams)


    def _add_matrix_group(self, props, split, family, viewmanager, viewstyle):
        cg = self.create_group_from_list([props['source'], props['destination'],
                props['dscp'], props['packet_size'], props['packet_spacing'],
                props['packet_count'], split, family.upper()])
        if cg is None:
            log("Failed to create %s group for %s matrix cell" % \
                    (family, self.collection_name))
            return -1

        viewid = viewmanager.add_groups_to_view(viewstyle, \
                self.collection_name, 0, [cg])

        if viewid is None:
            return -1
        return viewid


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
