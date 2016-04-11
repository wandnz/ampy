from libampy.database import AmpyDatabase
from libnntscclient.logger import *
import socket
import psycopg2

class ASNManager(object):

    def __init__(self, asdbconfig, cache):
        if asdbconfig is None:
            asdbconfig = {}
        if 'name' not in asdbconfig:
            asdbconfig['name'] = 'amp-asmap'

        self.dbconfig = asdbconfig
        self.db = AmpyDatabase(asdbconfig, True)
        self.db.connect(15)
        self.cache = cache;

    def queryDatabase(self, asn):

        query = "SELECT * FROM asmap WHERE asn=%s"
        params = (asn,)

        if self.db.executequery(query, params) == -1:
            log("Error while querying for AS name for %s" % (asn))
            return None

        if self.db.cursor.rowcount == 0:
            log("ASN %s not found in AS database :(" % (asn))
            self.db.closecursor()
            return "NotFound"

        asname = self.db.cursor.fetchone()['asname']
        self.db.closecursor()
        return asname

    def queryASNames(self, toquery):
        asnames = {}

        if len(toquery) == 0:
            return asnames

        for q in toquery:
            cached = self.cache.search_asname(q)
            if cached is not None:
                asnames[q] = cached
                continue

            queried = self.queryDatabase(q[2:])

            if queried is None:
                return None
            elif queried == "NotFound":
                asnames[q] = q
            else:
                self.cache.store_asname(q, queried)
                asnames[q] = queried

        return asnames

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
