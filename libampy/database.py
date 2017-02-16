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

import time
import psycopg2
import psycopg2.extras
from libnntscclient.logger import log

# Generic psycopg2 database code largely borrowed from NNTSC

class AmpyDatabase(object):
    """
    Helper class for interacting with postgresql databases.

    Acts as a wrapper around psycopg2 and manages cursors, connections,
    disconnects and error cases consistently so that we don't need to
    have the same database code in the views, events and ampmesh modules.

    API Functions
    -------------
    connect:
        Attempts to connect to the database. Will retry until the connection
        is successful.
    destroy:
        Tears down an existing database connection.
    reconnect:
        Attempts to reconnect to the database after dropping the existing
        connection first. Will retry until the connection succeeds.
    executequery:
        Executes the provided query using the current database connection.
        The database cursor may be used to access the query result.
    closecursor:
        Closes the database cursor. Should be called after processing of a
        query result is complete.
    commit:
        Commits the current transaction. Only necessary if the database has
        not been configured to auto-commit.
    """

    def __init__(self, dbconf, autocommit=False, name=None):
        """
        Init function for the AmpyDatabase class.

        Parameters:
          dbconf -- a dictionary describing the configuration options
                necessary for connecting to the database.
          autocommit -- a boolean flag indicating whether the transaction
                should be automatically committed after each query.
          name -- if not None, the database connection will use a
                server-side cursor for queries with the given name.
                Otherwise, a client-side cursor will be used.

        Database configuration options:
          name: the name of the database to connect to. Mandatory.
          user: the username to use when connecting. Defaults to the user
                who is running ampy.
          host: the host to connect to. Defaults to the host that ampy is
                running on.
          password: the password to use when connecting. Defaults to no
                password.

        """
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
        """
        Tears down an existing database connection.
        """
        if self.cursor is not None:
            self.cursor = None

        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connect(self, retrywait):
        """
        Connects to the database.

        If the connection attempt fails, this function will sleep for a short
        time and then try again.

        Parameters:
          retrywait -- the amount of time to sleep between connection attempts

        Returns:
          0 always.
        """
        logmessage = False

        while self.conn is None:
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
        """
        After a brief sleep, attempts to re-connect to the database.

        Will close any existing connections before reconnecting.
        """
        time.sleep(5)
        self.destroy()
        self.connect(5)

    def executequery(self, query, params):
        """
        Executes the given query against the current database.

        Parameters:
          query -- the query to run as a parameterised string
          params -- a tuple containing the parameters to substitute into
                    the query when run

        Returns:
          -1 if an error occurs, 0 if the query executes successfully
        """

        # Make sure we have a cursor available for the query
        if self.cursor is None:
            err = self._createcursor()
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
            return -1
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return -1
        except KeyboardInterrupt:
            return -1
        except psycopg2.Error as e:
            log(e)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return -1
        return 0

    def closecursor(self):
        """
        Closes the currently active cursor for the database.

        This should be run once processing of a query result is finished, as
        this will free up the cursor as per psycopg2 best practice.

        Returns:
          0 if successful, -1 if some database error prevents us from closing
          the cursor cleanly.
        """
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
            return -1
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return -1
        except KeyboardInterrupt:
            return -1
        except psycopg2.Error as e:
            log(e)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return -1
        self.cursor = None
        return 0

    def commit(self):
        """
        Commits the current transaction to the database.

        This should be run periodically if inserting data into the database
        and the auto-commit option is not set. If auto-commit is set, then
        don't bother calling this.

        Returns:
          0 if successful, -1 if some database error prevents us from
          committing.
        """
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
            return -1
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return -1
        except KeyboardInterrupt:
            return -1
        except psycopg2.Error as e:
            log(e)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return -1
        return 0

    def _createcursor(self):
        """
        Creates a cursor using our current database connection

        Returns -1 if cursor creation failed, 0 if successful
        """

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

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
