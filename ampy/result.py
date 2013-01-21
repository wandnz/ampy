#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Result object to allow easy fetching and use of results
"""

class Result(object):
    """ Object to represent and facilitate fetching of results """

    def __init__(self, data):
        """ Create a new result object and associate a data source with it.
            This datasource will be queried to provide result data as needed.

            Keyword arguments:
            data -- the source of data for the results
        """
        # Catch empty lists used as markers
        if data is None or len(data) == 0:
            self.data = None
        else:
            self.data = data
        self.index = 0

    def __iter__(self):
        return self

    def next(self):
        """ Fetch the next item for the iterator """
        if self.data is None:
            raise StopIteration
        if self.index < len(self.data):
            self.index += 1
            return self.data[self.index-1]
        raise StopIteration

    def fetchone(self):
        """ Fetch the next item in the result set, or None if all consumed """
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchall(self):
        """ Fetch all remaining items in the result set """
        if self.data is None:
            return []
        current = self.index
        self.index = len(self.data)
        return self.data[current:]

    def fetchmany(self, count):
        """ Fetch up to the specified number of results and return as a list """
        if self.data is None:
            return []
        i = 0
        result = []
        while i < count and self.index < len(self.data):
            result.append(self.data[self.index])
            self.index += 1
            i += 1
        return result

    def count(self):
        """ Return the number of results this query produced """
        if self.data is None:
            return 0
        return len(self.data)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
