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

        if asn == "" or int(asn) < 0:
            return None

        query = "SELECT * FROM asmap WHERE asn=%s"
        params = (asn,)

        if self.db.executequery(query, params) == -1:
            log("Error while querying for AS name for %s" % (asn))
            return None

        if self.db.cursor is None:
            log("Cursor for querying ASDB is None?")
            return None

        if self.db.cursor.rowcount == 0:
            log("ASN %s not found in AS database :(" % (asn))
            self.db.closecursor()
            return "NotFound"

        asname = self.db.cursor.fetchone()['asname']
        self.db.closecursor()
        return asname

    def getASNsByName(self, pagesize=30, offset=0, term=""):

        # sanitize the term so we don't get sql-injected


        query = """SELECT count(*) FROM asmap WHERE CAST(asn AS TEXT) ILIKE
                %s OR asname ILIKE %s"""
        params = ("%" + term + "%", "%" + term + "%")

        if self.db.executequery(query, params) == -1:
            log("Error while counting ASNs in the database")
            return (0, {})
        ascount = self.db.cursor.fetchone()[0]
        self.db.closecursor();

        query = """SELECT * FROM asmap WHERE CAST(asn AS TEXT) ILIKE
                %s OR asname ILIKE %s ORDER BY asn LIMIT %s OFFSET %s"""
        params = ("%" + term + "%", "%" + term + "%", pagesize, offset)

        if self.db.executequery(query, params) == -1:
            log("Error while querying for all AS names")
            return (0, {})

        allasns = []
        for row in self.db.cursor:
            asstring = "AS%s %s" % (row[0], row[1])
            allasns.append({'id': str(row[0]), 'text': asstring})

            if (len(allasns) > pagesize):
                break
        self.db.closecursor();
        return ascount, allasns


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
