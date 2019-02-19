import pymssql
import pandas as pd
import numpy as np
import sys
import json
from collections import OrderedDict

class mssql:

    conn = None
    credentials = None

    def __init__(self, server, database):
        with open('dbinfo.json') as f:
            self.credentials = json.load(f)
        self.server = server
        self.database = database
        self.queue_list = []
        self.connect(server, database)

    def __del__(self):
          self.conn.close()

    def connect(self, server, database):
        self.conn = pymssql.connect(server=server, user=self.credentials["username"], password=self.credentials["password"], database=database)
        self.cursor = self.conn.cursor()
        self.log(sys.argv[0])

    def query(self, qstring, results = True):
        if results:
            rows = self.cursor.execute(qstring).fetchall()
            columns = [column[0] for column in self.cursor.description]
            return columns, rows
        else:
            self.cursor.execute(qstring)
            self.conn.commit()

    def query_range(self, qstring):
        self.cursor.execute(qstring)
        row = self.cursor.fetchone()
        columns = [column[0] for column in self.cursor.description]
        while row:
            yield(OrderedDict(zip(columns, row)))
            row = self.cursor.fetchone()

    def singleton(self, qstring):
        for row in self.query_range(qstring):
            pass
        return row

    def insert(self, table, insert):
        q = self.make_insert(table, insert)
        self.query(q, False)

    def make_insert(self, table, insert):
        return "INSERT INTO " + table + " ([" + "],[".join([str(x) for x in insert]) + "]) VALUES ('" + "','".join([unicode(insert[x]).encode('utf-8').replace("'", "''") for x in insert]) + "')"

    def insert_unique(self, table, insert, unique_keys):
        unique_where = " AND ".join((x + " = '" + str(insert[x]) + "'" for x in unique_keys))
        q = "IF NOT EXISTS (SELECT 1 FROM " + table + " WHERE " + unique_where + ")"
        q = q + "INSERT INTO " + table + " ([" + "],[".join([str(x) for x in insert]) + "]) "
        q = q + "VALUES ('" + "','".join([unicode(insert[x]).replace("'", "''") for x in insert]) + "')"
        self.query(q, False)


    def queue(self, table, insert):
        self.queue_list.append(tuple(unicode(insert[x]) for x in insert))
        if len(self.queue_list) > 50:
            self.insert_queue(table, tuple(x for x in insert))

    def insert_queue(self, table, keys):
        if len(self.queue_list) > 0:
            self.insert_many(table, keys, self.queue_list)
            self.queue_list = []

    def insert_many(self, table, keys, params):
        q = "INSERT INTO " + table + " ([" + "],[".join([str(x) for x in keys]) + "]) VALUES (" + ','.join("?" * len(keys)) + ")"
        self.cursor.executemany(q, params)
        self.conn.commit()

    def log(self, entry):
        pass
        # insert = dict([("entry", entry)])
        # self.insert("cronlog", insert)
