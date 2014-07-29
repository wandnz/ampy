from libnntscclient.logger import *
from operator import itemgetter
from libampy.collections.lpipackets import LPIPackets

class LPIBytes(LPIPackets):
    def __init__(self, colid, viewmanager, nntscconf):
        super(LPIBytes, self).__init__(colid, viewmanager, nntscconf)
        self.collection_name = "lpi-bytes"
        self.metric = "bytes"
        self.viewstyle = self.collection_name

    def detail_columns(self, detail):
        return ['bytes'], ['avg']

    def format_single_data(self, data, freq):
        if 'bytes' not in data:
            return data

        if data['bytes'] == None:
            data['mbps'] = None
        else:
            data['mbps'] = ((float(data['bytes']) * 8.0) / 1000000.0) / freq
        
        return data

    def format_list_data(self, datalist, freq):
        for d in datalist:
            self.format_single_data(d, freq)
        return datalist

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
