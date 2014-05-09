import psycopg2
import psycopg2.extras
import string
from libnntscclient.logger import *

# Generic psycopg2 database code largely borrowed from NNTSC

class AmpyDatabase(object):
    def __init__(self, dbconf, autocommit=False, name=None):
        self.cursorname = name
        self.autocommit = autocommit

        self.conn = None
        self.cursor = None

        assert('name' in dbconf)
        self.dbname = dbconf["name"]

        connstr = "dbname=%s" % (dbconf['name'])
        if "user" in dbconf:
            connstr += " user=%s" % (dbconf["user"])
        if "password" in dbconf:
            connstr += " password=%s" % (dbconf["password"])
        if "host" in dbconf:
            connstr += " host=%s" % (dbconf["host"])
 
        self.connstr = connstr

    def destroy(self):
        if self.cursor is not None:
            self.cursor = None

        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connect(self, retrywait):
        logmessage = False

        while self.conn == None:
            try:
                self.conn = psycopg2.connect(self.connstr)
            except psycopg2.DatabaseError as e:
                if not logmessage:
                    log("Error connecting to %s database: %s" % (self.dbname, e))
                    log("Retrying every %d seconds" % retrywait)
                    logmessage = True
                self.conn = None
                time.sleep(retrywait)

        self.conn.autocommit = self.autocommit

        if logmessage:
            log("Successfully connected to database %s" % (self.dbname))

        self.cursor = None
        return 0

    def reconnect(self):
        time.sleep(5)
        self.destroy()
        self.connect(5)

    def createcursor(self):

        try:
            if self.cursorname is not None:
                self.cursor = self.conn.cursor(self.cursorname,
                        cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                self.cursor = self.conn.cursor(
                        cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.OperationalError as e:
            log("Database %s disconnect while resetting cursor" % (self.dbname))
            self.cursor = None
            return -1
        except psycopg2.DatabaseError as e:
            log("Failed to create cursor: %s" % e)
            self.cursor = None
            return -1

        return 0

    def executequery(self, query, params):
        if self.cursor is None:
            err = self.createcursor()
            if err != 0:
                return err

        try:
            if params is not None:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            return -1
        except psycopg2.OperationalError:
            log("Database %s appears to have disappeared -- reconnecting" % (self.dbname))
            self.reconnect()
            return -1
        except psycopg2.ProgrammingError as e:
            log(e)
            self.conn.rollback()
            return -1
        except psycopg2.IntegrityError as e:
            log(e)
            self.conn.rollback()
            if " duplicate " in str(e):
                return -1
            return -1
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return -1
        except KeyboardInterrupt:
            return -1
        except psycopg2.Error as e:
            log(e.pgerror)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return -1
        return 0

    def closecursor(self):
        if self.cursor is None:
            return 0

        if self.conn is None:
            self.cursor = None
            return 0

        try:
            self.cursor.close()
            err = 0
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            return -1
        except psycopg2.OperationalError:
            log("Database %s appears to have disappeared -- reconnecting" % (self.dbname))
            self.reconnect()
            return -1
        except psycopg2.ProgrammingError as e:
            log(e)
            self.conn.rollback()
            return -1
        except psycopg2.IntegrityError as e:
            log(e)
            self.conn.rollback()
            if " duplicate " in str(e):
                return -1
            return -1
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return -1
        except KeyboardInterrupt:
            return -1
        except psycopg2.Error as e:
            log(e.pgerror)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return -1
        return 0


    def commit(self):
        if self.conn is None:
            return -1

        try:
            self.conn.commit()
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            return -1
        except psycopg2.OperationalError:
            log("Database %s appears to have disappeared -- reconnecting" % (self.dbname))
            self.reconnect()
            return -1
        except psycopg2.ProgrammingError as e:
            log(e)
            self.conn.rollback()
            return -1
        except psycopg2.IntegrityError as e:
            log(e)
            self.conn.rollback()
            if " duplicate " in str(e):
                return -1
            return -1
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return -1
        except KeyboardInterrupt:
            return -1
        except psycopg2.Error as e:
            log(e.pgerror)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return -1
        return 0

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
