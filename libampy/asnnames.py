#
# This file is part of ampy.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# ampy is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# ampy is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ampy; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#

from threading import Lock
from libampy.database import AmpyDatabase
from libnntscclient.logger import log

class ASNManager(object):
    def __init__(self, asdbconfig, cache):
        if asdbconfig is None:
            asdbconfig = {}
        if 'name' not in asdbconfig:
            asdbconfig['name'] = 'amp-asmap'

        self.dbconfig = asdbconfig
        self.db = AmpyDatabase(asdbconfig, True)
        self.db.connect(15)
        self.cache = cache
        self.dblock = Lock()

    def queryDatabase(self, asn):
        if asn == "" or int(asn) < 0:
            return None

        query = "SELECT * FROM asmap WHERE asn=%s"
        params = (asn,)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            self.dblock.release()
            log("Error while querying for AS name for %s" % (asn))
            return None

        if self.db.cursor is None:
            self.dblock.release()
            log("Cursor for querying ASDB is None?")
            return None

        if self.db.cursor.rowcount < 1:
            self.db.closecursor()
            self.dblock.release()
            log("ASN %s not found in AS database :(" % (asn))
            return "NotFound"

        asname = self.db.cursor.fetchone()['asname']
        self.db.closecursor()
        self.dblock.release()
        return asname

    def getASNsByName(self, pagesize=30, offset=0, term=""):
        # sanitize the term so we don't get sql-injected
        query = """SELECT count(*) FROM asmap WHERE CAST(asn AS TEXT) ILIKE
                %s OR asname ILIKE %s"""
        params = ("%" + term + "%", "%" + term + "%")

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            self.dblock.release()
            log("Error while counting ASNs in the database")
            return (0, {})
        ascount = self.db.cursor.fetchone()[0]
        self.db.closecursor()
        self.dblock.release()

        query = """SELECT * FROM asmap WHERE CAST(asn AS TEXT) ILIKE
                %s OR asname ILIKE %s ORDER BY asn LIMIT %s OFFSET %s"""
        params = ("%" + term + "%", "%" + term + "%", pagesize, offset)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            self.dblock.release()
            log("Error while querying for all AS names")
            return (0, {})

        allasns = []
        for row in self.db.cursor:
            asstring = "AS%s %s" % (row[0], row[1])
            allasns.append({'id': str(row[0]), 'text': asstring})

            if len(allasns) > pagesize:
                break
        self.db.closecursor()
        self.dblock.release()
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
