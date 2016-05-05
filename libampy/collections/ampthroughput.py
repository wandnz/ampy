from libnntscclient.logger import *
from libampy.collection import Collection
from operator import itemgetter

class AmpThroughput(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpThroughput, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'destination',  
                'duration', 'writesize', 'tcpreused', 'direction', 'family']
        self.groupproperties = self.streamproperties
        self.integerproperties = ['duration', 'writesize']
        self.collection_name = "amp-throughput"
        self.viewstyle = "amp-throughput"

        self.default_duration = 10000
        self.default_writesize = 131072

        self.dirlabels = {"in": "Download", "out": "Upload"}

    def detail_columns(self, detail):

        aggfuncs = ["sum", "sum", "sum"]
        aggcols = ["bytes", "packets", "runtime"]

        return aggcols, aggfuncs

    def calculate_binsize(self, start, end, detail):
        # Hard to pre-determine a suitable binsize for throughput tests
        # as the measurement frequency is likely to change from test to test.
        # Problem is, if we choose a bad binsize we can easily end up in a
        # situation where we think there's a gap in the data when there 
        # really isn't
        if (end - start) / 3600 < 200:
            return 3600

        if (end - start) / (3600 * 4) < 200:
            return (3600 * 4)

        if (end - start) / (3600 * 12) < 200:
            return (3600 * 12)

        return (3600 * 24)

    def prepare_stream_for_storage(self, stream):
        if 'address' not in stream:
            return stream, {}

        # XXX Can local and remote addresses be different families?
        if self._address_to_family(stream['address']) == "ipv4":
            stream['family'] = "ipv4"
        else:
            stream['family'] = "ipv6"

        return stream, {'address':stream['address']}

    def create_group_description(self, properties):
        if 'tcpreused' in properties:
            if properties['tcpreused'] == True:
                reuse = "T"
            else:
                reuse = "F"
        
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

        return "FROM %s TO %s DURATION %s WRITESIZE %s %s DIRECTION %s FAMILY %s" \
                % (properties['source'], properties['destination'], 
                   properties['duration'], properties['writesize'], reuse,
                   properties['direction'], properties['family'])

    def get_legend_label(self, description):
        gps = self.parse_group_description(description)
        if gps is None:
            log("Failed to parse group description to generate legend label")
            return None

        if gps["tcpreused"] == True:
            reuse = "+reuse"
        else:
            reuse = ""

        source = gps['source']
        dest = gps['destination']

        if gps['family'] == "IPV4":
            family = "IPv4"
        elif gps['family'] == "IPV6":
            family = "IPv6"
        elif gps['family'] == "BOTH":
            family = "IPv4/IPv6"
        else:
            family = ""
        
        durationsecs = gps['duration'] / 1000.0
        kilobytes = gps['writesize'] / 1024.0


        if gps['direction'] == "BOTH":
            dirstr = ""
        elif gps['direction'] == "IN":
            dirstr = " Download"
        else:
            dirstr = " Upload"
    
        label = "%s : %s for %.1f secs, %.1f kB writes" % (source, dest,
                durationsecs, kilobytes)

        return label, "%s%s" % (family, dirstr)

    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9-:]+) "
        regex += "DURATION (?P<duration>[0-9]+) "
        regex += "WRITESIZE (?P<writesize>[0-9]+) "
        regex += "(?P<reused>[TF]) "
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
            'source':  parts.group("source"),
            'destination': parts.group("destination"),
            'family': parts.group("family"),
            'direction': parts.group("direction"),
            'duration': int(parts.group("duration")),
            "writesize": int(parts.group("writesize")),
        }

        if parts.group("reused") == 'T':
            keydict['tcpreused'] = True
        else:
            keydict['tcpreused'] = False

        return keydict

    def _generate_direction_labels(self, baselabel, search, direction, family, 
            lookup):
        key = baselabel + "_" + direction
        search['direction'] =  direction

        if direction in self.dirlabels:
            shortlabel = self.dirlabels[direction]
        else:
            shortlabel = ""

        labels = []

        if family in ["BOTH", "IPV4"]:
            lab = self._generate_family_label(key, search, "IPv4", lookup)
            if lab is None:
                return None
            labels.append(lab)
        
        if family in ["BOTH", "IPV6"]:
            lab = self._generate_family_label(key, search, "IPv6", lookup)
            if lab is None:
                return None
            labels.append(lab)

        if family == "NONE":
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for %s, %s" % \
                        (key, self.collection_name))

            for sid, store in streams:
                if 'local' not in store or 'remote' not in store:
                    log("Error: no addresses stored with stream id %s" % (sid))
                    return None

                if shortlabel == "in":
                    streamlabel = "%s to %s" % (store['remote'], store['local'])
                else:
                    streamlabel = "%s to %s" % (store['local'], store['remote'])

                lab = {'labelstring':key + "_" + str(sid),
                        'streams':[sid], 'shortlabel':streamlabel}
                labels.append(lab)

        return labels
                    
    def _generate_family_label(self, baselabel, search, family, lookup):
        key = baselabel + "_" + family
        search['family'] = family.lower()
        if search['direction'] in self.dirlabels:
            shortlabel = family + " " + self.dirlabels[search['direction']]
        else:
            shortlabel = family

        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None

            famstreams = [item[0] for item in streams]
        else:
            famstreams = []

        return {'labelstring':key, 'streams':famstreams,
                'shortlabel':shortlabel}

    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate group labels")
            return None

        baselabel = 'group_%s' % (groupid)

        search = dict(groupparams)
        del search['direction']
        del search['family']

        if groupparams['direction'] in ['IN', 'BOTH']:
            lab = self._generate_direction_labels(baselabel, search, 'in', 
                    groupparams['family'], lookup)
            if lab is None:
                return None
            labels += lab


        if groupparams['direction'] in ['OUT', 'BOTH']:
            lab = self._generate_direction_labels(baselabel, search, 'out', 
                    groupparams['family'], lookup)
            if lab is None:
                return None
            labels += lab

        return sorted(labels, key=itemgetter('shortlabel'))

    def update_matrix_groups(self, source, dest, split, groups, views,
            viewmanager):
        groupprops = {'source': source, 'destination': dest, 
                'duration':self.default_duration, 
                'writesize': self.default_writesize, 'tcpreused': False,
                }

        tputin4 = self._matrix_group_streams(groupprops, "in", "ipv4", groups)
        tputout4 = self._matrix_group_streams(groupprops, "out", "ipv4", groups)
        tputin6 = self._matrix_group_streams(groupprops, "in", "ipv6", groups)
        tputout6 = self._matrix_group_streams(groupprops, "out", "ipv6", groups)


        if tputin4 == 0 and tputout4 == 0:
            views[(source, dest, "ipv4")] = -1

        if tputin6 == 0 and tputout6 == 0:
            views[(source, dest, "ipv6")] = -1

        if tputin4 + tputin6 + tputout4 + tputout6 == 0:
            return

        if split == "down":
            split = "IN"
        elif split == "up":
            split = "OUT"
        else:
            split = "BOTH"

        if tputin4 != 0 or tputout4 != 0:
            # XXX this could become a function
            cg = self.create_group_from_list([source, dest, 
                    self.default_duration,
                    self.default_writesize, False, split, "IPV4"])
            if cg is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                return None

            viewid = viewmanager.add_groups_to_view(self.viewstyle,
                    self.collection_name, 0, [cg])

            if viewid is None:
                views[(source, dest, "ipv4")] = -1
            else:
                views[(source, dest, "ipv4")] = viewid

        if tputin6 != 0 or tputout6 != 0:
            cg = self.create_group_from_list([source, dest, 
                    self.default_duration,
                    self.default_writesize, False, split, "IPV6"])
            if cg is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                return None

            viewid = viewmanager.add_groups_to_view(self.viewstyle,
                    self.collection_name, 0, [cg])

            if viewid is None:
                views[(source, dest, "ipv6")] = -1
            else:
                views[(source, dest, "ipv6")] = viewid


    def _matrix_group_streams(self, baseprops, direction, family, groups):

        baseprops['direction'] = direction
        baseprops['family'] = family
        label = "%s_%s_%s_%s" % (baseprops['source'], baseprops['destination'],
                direction, family)
        streams = self.streammanager.find_streams(baseprops)

        if len(streams) > 0:
            groups.append({'labelstring':label, 'streams': [x[0] for x in streams]})

        return len(streams)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
