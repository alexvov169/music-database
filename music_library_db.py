import psycopg2
DATABASE_DATA = {'entities': [{'name': 'Artist',
                               'attributes': [{'name': 'Artist_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Artist_Id',
                                               'type': 'INT'}]},
                              {'name': 'Album',
                               'attributes': [{'name': 'Album_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Album_Id',
                                               'type': 'INT'},
                                              {'name': 'Year',
                                               'type': 'INT'}]},
                              {'name': 'Playlist',
                               'attributes': [{'name': 'Filepath',
                                               'type': 'VARCHAR'},
                                              {'name': 'Description',
                                               'type': 'VARCHAR'},
                                              {'name': 'Playlist_Name',
                                               'type': 'VARCHAR'}]},
                              {'name': 'Track',
                               'attributes': [{'name': 'Track_Name',
                                               'type': 'VARCHAR'},
                                              {'name': 'Track_Id',
                                               'type': 'INT'},
                                              {'name': 'Number',
                                               'type': 'INT'}]},
                              {'name': 'release',
                               'attributes': [{'name': 'Track_Id',
                                               'type': 'INT'},
                                              {'name': 'Artist_Id',
                                               'type': 'INT'}]}]}


class MusicLibraryDatabase:
    _SQL_CREATE_SCRIPT = """
    CREATE TABLE Artist
    (
      Artist_Name VARCHAR(256) NOT NULL,
      Artist_Id INT NOT NULL,
      PRIMARY KEY (Artist_Id)
    );

    CREATE TABLE Album
    (
      Album_Name VARCHAR(256) NOT NULL,
      Album_Genre VARCHAR(256) NOT NULL,
      Album_Id INT NOT NULL,
      Year INT NOT NULL,
      PRIMARY KEY (Album_Id)
    );

    CREATE TABLE Playlist
    (
      Filepath VARCHAR(4096) NOT NULL,
      Description VARCHAR(4096) NOT NULL,
      Playlist_Name VARCHAR(256) NOT NULL,
      PRIMARY KEY (Filepath)
    );

    CREATE TABLE release
    (
      Artist_Id INT NOT NULL,
      Album_Id INT NOT NULL,
      PRIMARY KEY (Artist_Id, Album_Id),
      FOREIGN KEY (Artist_Id) REFERENCES Artist(Artist_Id),
      FOREIGN KEY (Album_Id) REFERENCES Album(Album_Id)
    );

    CREATE TABLE Track
    (
      Track_Name VARCHAR(256) NOT NULL,
      Track_Id INT NOT NULL,
      Number INT NOT NULL,
      Album_Id INT NOT NULL,
      Filepath VARCHAR(4096) NOT NULL,
      PRIMARY KEY (Track_Id),
      FOREIGN KEY (Album_Id) REFERENCES Album(Album_Id),
      FOREIGN KEY (Filepath) REFERENCES Playlist(Filepath)
    );
    """

    def __init__(self, files, user, host, password, database):
        self.connector = psycopg2.connect("dbname='music_library' user='postgres' host='localhost' password='py'")
        #psycopg2.connect(user=user, host=host, password=password, database=database)
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
        self._execute("INSERT INTO %s %s VALUES %s", into, row)

    def delete_all(self, entity):
        self.cursor.execute("DELETE FROM %s;" % entity)

    def edit(self, what, where, replacement):
        pass

    def drop_table(self, table):
        pass

    def drop_db(self):
        pass

    def fulltext_not_occurred(self, table, text):
        self.cursor.execute("""SELECT * FROM %s WHERE MATCH(col1, col2)
               AGAINST('search terms' IN BOOLEAN MODE)""")

    @staticmethod
    def _row_to_condition(row, op="OR"):
        row = [[k, v] for k, v in row.items()]
        s = "%s=%s " % (row[0][0], row[0][1])
        for k, v in row[1:]:
            s = s + op + " %s=%s" % (k, v)
        return s

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
            'Artist': 'Artist_Id',
            'Album': 'Album_Id',
            'Track': 'Track_Id',
            'release': {
                'Track_Id',
                'Artist_Id'
            }
        }
        return key_attrs[entity]

    def select_inner_join(self, entity1, entity2):
        key_attr = self._get_key(entity1)
        if not isinstance(key_attr, str):
            key_attr = self._get_key(entity2)
            if not isinstance(key_attr, str):
                raise Exception()

        query = "SELECT * FROM %s INNER JOIN %s ON %s.%s = %s.%s"\
                % (entity1, entity2,
                   entity1, key_attr,
                   entity2, key_attr)
        print(query)
        self.cursor.execute(query)
        return self._fetch_all()

    def fulltext_search_all_match(self, entity, attribute, key):
        query = """select * from %s WHERE to_tsvector(%s) @@ to_tsquery('%s');"""
        query = query % (entity, attribute, ' & '.join(key.split()))
        print(query)
        self.cursor.execute(query, (entity, attribute, key))
        return self._fetch_all()

    def fulltext_search_all_match_query_from_plaintext(self, entity, attribute, key):
        query = """select * from %s WHERE to_tsvector(%s) @@ plainto_tsquery('%s');"""
        query = query % (entity, attribute, key)
        print(query)
        self.cursor.execute(query, (entity, attribute, key))
        return self._fetch_all()

    def fulltext_search_one_not(self, entity, attribute, key):
        query = """select * from %s WHERE to_tsvector(%s) @@ to_tsquery('!%s');"""
        query = query % (entity, attribute, key)
        print(query)
        self.cursor.execute(query, (entity, attribute, key))
        return self._fetch_all()
