import psycopg2
joins = [
    {'pair': {'has', 'Tag'},
     'id': 'Tag_Id'},
    {'pair': {'Track', 'Album'},
     'id': 'Album_Id'},
    {'pair': {'Album', 'has'},
     'id': 'Album_Id'},
    {'pair': {'Album', 'Artist'},
     'id': 'Artist_Id'}
]

DATABASE_DATA = {'entities': [{'name': 'Artist',
                               'attributes': [{'name': 'Artist_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Artist_Description',
                                               'type': 'VARCHAR'},
                                              {'name': 'Artist_Id',
                                               'type': 'INT'}]},
                              {'name': 'Album',
                               'attributes': [{'name': 'Album_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Album_Id',
                                               'type': 'INT'},
                                              {'name': 'Year',
                                               'type': 'INT'},
                                              {'name': 'Artist_Id',
                                               'type': 'INT'},
                                              ]},
                              {'name': 'Tag',
                               'attributes': [{'name': 'Tag_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Tag_Description',
                                               'type': 'VARCHAR'},
                                              {'name': 'Tag_Id',
                                               'type': 'INT'}]},
                              {'name': 'has',
                               'attributes': [{'name': 'Album_Id',
                                               'type': 'INT'},
                                              {'name': 'Tag_Id',
                                               'type': 'INT'}]},
                              {'name': 'Track',
                               'attributes': [{'name': 'Track_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Track_Id',
                                               'type': 'INT'},
                                              {'name': 'Rank',
                                               'type': 'INT'},
                                              {'name': 'Duration',
                                               'type': 'INT'},
                                              {'name': 'Album_Id',
                                               'type': 'INT'}]}]}

_SQL_CREATE_TABLE_SCRIPT = """
CREATE TABLE Artist
(
  Artist_Name VARCHAR(128) NOT NULL,
  Artist_Description VARCHAR(4096) NOT NULL,
  Artist_Id SERIAL NOT NULL,
  PRIMARY KEY (Artist_Id)
);

CREATE TABLE Album
(
  Album_Name VARCHAR(128) NOT NULL,
  Album_Id SERIAL NOT NULL,
  Year INT NOT NULL,
  Artist_Id SERIAL NOT NULL,
  PRIMARY KEY (Album_Id),
  FOREIGN KEY (Artist_Id) REFERENCES Artist(Artist_Id)
);

CREATE TABLE Tag
(
  Tag_Id SERIAL NOT NULL,
  Tag_Name VARCHAR(128) NOT NULL,
  Tag_Description VARCHAR(4096) NOT NULL,
  PRIMARY KEY (Tag_Id)
);

CREATE TABLE has
(
  Album_Id SERIAL NOT NULL,
  Tag_Id SERIAL NOT NULL,
  PRIMARY KEY (Album_Id, Tag_Id),
  FOREIGN KEY (Album_Id) REFERENCES Album(Album_Id),
  FOREIGN KEY (Tag_Id) REFERENCES Tag(Tag_Id)
);

CREATE TABLE Track
(
  Track_Name VARCHAR(128) NOT NULL,
  Track_Id SERIAL NOT NULL,
  Rank INT NOT NULL,
  Duration INT NOT NULL,
  Album_Id SERIAL NOT NULL,
  PRIMARY KEY (Track_Id),
  FOREIGN KEY (Album_Id) REFERENCES Album(Album_Id)
);
"""


class MusicLibraryDatabase:
    _SQL_CREATE_SCRIPT = _SQL_CREATE_TABLE_SCRIPT

    def __init__(self, user, host, password, database):
        self.connector = psycopg2.connect(user=user, host=host, password=password, database=database)
        # psycopg2.connect("dbname='db1' user='postgres' host='localhost' password='py'")
        self.cursor = self.connector.cursor()
        #try:
            #self.cursor.execute(self._SQL_CREATE_SCRIPT)
        #except:
        #    pass
        #finally:
        #    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connector.commit()
        self.cursor.close()
        self.connector.close()

    @staticmethod
    def _get_what_where_query_format(row):
        value_list = []
        where_s = '('
        what_sf = '('
        for k, v in row.items():
            where_s = where_s + k + ', '
            what_sf = what_sf + '%s, '
            value_list.append(v)
        return where_s[:-2] + ')', what_sf[:-2] + ')', value_list

    def _execute(self, query, entity, row):
        where_s, what_sf, value_list = self._get_what_where_query_format(row)
        self.cursor.execute((query % (entity, where_s, what_sf)), value_list)

    def _fetch_all(self):
        try:
            return self.cursor.fetchall()
        except psycopg2.ProgrammingError:
            return []

    def fetch_all(self):
        return self._fetch_all()

    def insert(self, into, row):
        # self._execute("INSERT INTO %s %s VALUES %s", into, row)
        def make_values(values):
            # print(values)
            return "(%s)" % (', '.join([("'%s'" %
                                         value.replace('"', '\' || CHR(39) || \'').replace("'", '\' || CHR(39) || \''))
                                        if isinstance(value, str)
                                        else str(value)
                                        for value in values]))

        def make_keys(keys):
            return "(%s)" % (', '.join(keys))

        keys = [key for key, value in row.items()]
        values = [value for key, value in row.items()]
        query = "INSERT INTO %s %s VALUES %s" % (into, make_keys(keys), make_values(values))
        # print(query)
        self.cursor.execute(query)

    def delete_all(self, entity):
        self.cursor.execute("DELETE FROM %s;" % entity)

    def drop_table(self, table):
        pass

    def drop_db(self):
        pass

    def fulltext_not_occurred(self, table, text):
        """mysql; not working"""
        self.cursor.execute("""SELECT * FROM %s WHERE MATCH(col1, col2)
               AGAINST('search terms' IN BOOLEAN MODE)""")

    @staticmethod
    def _kv_to_sql(k, v):
        return ("%s=%s" if isinstance(v, int) else "%s='%s'") % (k, v)

    @staticmethod
    def _kv_to_sql_with_entity(e, k, v):
        cond = (("%s.%s=%s" % (e, k, v)) if isinstance(v, int) else
                ("%s.%s='%s'" % (e, k, v)) if isinstance(v, str) else
                ("%s.%s >= %s AND %s.%s <= %s" % (e, k, v['lower'], e, k, v['upper']))
                if isinstance(v, dict) else "")
        return cond

    def _row_to_condition(self, row, op=" OR "):
        row = [[k, v] for k, v in row.items()]
        condition = op.join(self._kv_to_sql(*kv) for kv in row)
        return condition

    def delete(self, entity, row):
        self._execute("DELETE FROM %s WHERE CustomerName='Alfreds Futterkiste';", entity, row)
        pass

    def select_all(self, entity):
        self.cursor.execute("SELECT * FROM %s;" % entity)
        return self._fetch_all()

    def select(self, entity, attribute, *values):
        self.cursor.execute(("SELECT * FROM %s WHERE %s IN %%s;" % (entity, attribute)), *values)

    @staticmethod
    def _get_key(entity):
        key_attrs = {
            'Artist': {'Artist_Id'},
            'Album': {'Album_Id'},
            'Track': {'Track_Id'},
            'Tag': {'Tag_Id'},
            'has': {
                'Album_Id',
                'Tag_Id'
            }
        }
        return key_attrs[entity]

    def get_key(self, entity):
        return self._get_key(entity)

    def _dict_list_to_condition(self, entity, dict_list, op=' AND '):
        return op.join(op.join(self._kv_to_sql_with_entity(entity, k, v) for k, v in d.items())
                       for d in dict_list)

    def select_inner_join(self, entity1, entity2, key_attr, entity1_key_val, entity2_key_val):
        condition_1 = self._dict_list_to_condition(entity1, entity1_key_val)
        condition_2 = self._dict_list_to_condition(entity2, entity2_key_val)

        condition = ' and '.join((condition_1, condition_2))

        query = "SELECT * FROM %s INNER JOIN %s ON %s.%s = %s.%s WHERE %s;"\
                % (entity1, entity2,
                   entity1, key_attr,
                   entity2, key_attr,
                   condition)
        print('psql> '+query)
        self.cursor.execute(query)
        return self._fetch_all()

    def fulltext_search_all_match(self, entity, attribute, key):
        query = """select * from %s WHERE to_tsvector(%s) @@ to_tsquery('%s');"""
        query = query % (entity, attribute, ' & '.join(key.split()))
        print('psql> '+query)
        self.cursor.execute(query, (entity, attribute, key))
        return self._fetch_all()

    def fulltext_search_all_match_query_from_plaintext(self, entity, attribute, key):
        query = """select * from %s WHERE to_tsvector(%s) @@ plainto_tsquery('%s');"""
        query = query % (entity, attribute, key)
        print('psql> '+query)
        self.cursor.execute(query, (entity, attribute, key))
        return self._fetch_all()

    def fulltext_search_one_not(self, entity, attribute, key):
        query = """select * from %s WHERE to_tsvector(%s) @@ to_tsquery('!%s');"""
        query = query % (entity, attribute, key)
        print('psql> '+query)
        self.cursor.execute(query, (entity, attribute, key))
        return self._fetch_all()

    def update(self, entity, key, row):
        query = "UPDATE %s SET %s WHERE %s;"
        assignment = self._row_to_condition(row, op=", ")
        condition = self._row_to_condition(key, op=' AND ')
        query = query % (entity, assignment, condition)
        print('psql> '+query)
        self.cursor.execute(query)
        return self._fetch_all()