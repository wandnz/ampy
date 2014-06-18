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

    def detail_columns(self, detail):

        aggfuncs = ["sum", "sum", "sum"]
        aggcols = ["bytes", "packets", "runtime"]

        return aggcols, aggfuncs

    def prepare_stream_for_storage(self, stream):
        if 'remoteaddress' not in stream:
            return stream, {}

        # XXX Can local and remote addresses be different families?
        if self._address_to_family(stream['remoteaddress']) == "ipv4":
            stream['family'] = "ipv4"
        elif self._address_to_family(stream['localaddress']) == "ipv4":
            stream['family'] = "ipv4"
        else:
            stream['family'] = "ipv6"

        return stream, {'local':stream['localaddress'], 
                'remote':stream['remoteaddress']}

    def create_group_description(self, properties):
        if 'tcpreused' in properties:
            if properties['tcpreused'] == True:
                reuse = "T"
            else:
                reuse = "F"
        
        if 'direction' not in properties:
            properties['direction'] = "BOTH"
        if 'family' not in properties and 'remoteaddress' in properties:
            properties['family'] = \
                    self._address_to_family(properties['remoteaddress'])

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


        label = "%s : %s for %.1f secs, %.1f kB writes %s %s" % (source, dest,
                durationsecs, kilobytes, gps['direction'], family)

        return label 

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
        shortlabel = direction

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
        shortlabel = family + " " + search['direction']

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

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
