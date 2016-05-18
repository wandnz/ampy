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
        self.default_count = 11
        self.default_dscp = "Default"

        self.dirlabels = {"in": "Inward", "out": "Outward"}

    def extra_blocks(self, detail):
        if detail in ["jitter", "full"]:
            return 2
        return 0;

    def detail_columns(self, detail):

        if detail == "jitter" or detail == "jitter-summary":
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

        return "FROM %s TO %s DSCP %s SIZE %s SPACING %s COUNT %s DIRECTION %s FAMILY %s" \
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
        elif gps['family'] == "BOTH":
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
        regex += "FAMILY (?P<family>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('direction') not in ['IN', 'OUT', 'BOTH']:
            log("%s is not a valid direction for a %s group" % \
                    (parts.group('direction'), self.collection_name))
            return None

        if parts.group('family') not in ['IPV4', 'IPV6', 'BOTH', 'NONE']:
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
            viewmanager):

        # TODO
        return



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
