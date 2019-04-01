# import pylast
# import tqdm
# import functools
import csv
import json
import requests

import pyprind
import terminaltables
from music_library_db import *

scrobbler_root = "http://ws.audioscrobbler.com/2.0/"

# yeah, i give it to you
api_account_details = {
 'api_key': '4a6facf7352d482ddd5a5cf8b7dd88bf',
 'api_secret': '94a63b6915912ad000415fe27d8833db',
 'username': 'monty-1'
}


def options_to_query_string(**options):
    return ''.join(['&%s=%s' % (k, v) for k, v in options.items()])


def last_get(method, api_key, options, file_format='json'):
    req = scrobbler_root+'?method='+method+'&api_key='+api_key +\
          options_to_query_string(**options)+'&format='+file_format
    # print(req)
    return requests.get(req)


def get_tag_description(tag_name):
    description_resp = last_get(method="tag.getinfo", api_key=api_account_details['api_key'],
                                options={'tag': tag_name})
    description_json = description_resp.json()
    return description_json['tag']['wiki']['summary']


TAGS_LIMIT = 5


def published_date_to_year(published_date):
    return int(published_date.split()[2][:-1])


def get_album(album, artist_name):
    album_name = album['name']
    response = last_get(method="album.getinfo", api_key=api_account_details['api_key'],
                        options={'artist': artist_name, 'album': album_name})
    try:
        album_info = response.json()['album']
    except:
        return None
    else:
        tracks = list(map(lambda track: {"name": track['name'],
                                         "duration": track['duration'],
                                         "rank": track['@attr']['rank']},
                          album_info['tracks']['track']))
        try:
            published_info = album_info['wiki']['published']
            tags_resp = last_get(method='album.gettoptags', api_key=api_account_details['api_key'],
                                 options={'artist': artist_name, 'album': album_name})
            tags_json = tags_resp.json()
            tags = tags_json['toptags']['tag'][:TAGS_LIMIT]
            bar = pyprind.ProgBar(TAGS_LIMIT, title='Fetching album %s description' % album_name)

            def tag_proc(tag):
                bar.update()
                return {'name': tag['name'], 'description': get_tag_description(tag['name'])}

            tags = [tag_proc(tag) for tag in tags]
        except:
            return None
        else:
            return {"name": album_name,
                    "listeners": album_info['listeners'],
                    "playcount": album['playcount'],
                    "published": published_date_to_year(published_info),
                    "tracks": tracks,
                    "tags": tags}


def get_top_albums(artist_name):
    response = last_get(method="artist.getTopAlbums", api_key=api_account_details['api_key'],
                        options={'artist': artist_name, 'limit': '5'})
    return list(filter(lambda x: x is not None,
                       map(lambda album: get_album(album, artist_name),
                           response.json()['topalbums']['album'])))


def get_artist_description(artist_name):
    response = last_get(method="artist.getinfo", api_key=api_account_details['api_key'],
                        options={'artist': artist_name})
    description_json = response.json()
    return description_json['artist']['bio']['summary']


def make_artist_with_tracks(artist):
    return {"artist": artist,
            "albums": get_top_albums(artist['name']),
            "description": get_artist_description(artist['name'])}


def dump_artists_json(file, country="United States", artists_limit=10):
    artists_get_response = last_get(method="geo.getTopArtists", api_key=api_account_details['api_key'],
                                    options={'country': country, 'limit': artists_limit})

    top_artists_json = artists_get_response.json()
    artists = top_artists_json['topartists']['artist']
    artists_json = [{"name": artist['name'], "listeners": artist['listeners']}
                    for artist in artists]

    artists_json.sort(key=lambda artist: int(artist['listeners']), reverse=False)
    print("SORTED")
    artists_json = list(map(make_artist_with_tracks, artists_json))

    with open(file, 'w', encoding='utf-8') as f:
        json.dump({"topartists": artists_json}, f, indent=True)


def dict_to_csv(j):
    # print(j)
    return ';'.join([dict_to_csv(v) if isinstance(v, dict)
                     else ';'.join([dict_to_csv(i) for i in v]) if isinstance(v, list)
                     else str(v)
                     for k, v in j.items()])


def rows_to_csv(rows, file):
    with open(file, 'w', encoding='utf-8') as out:
        csv_data = '\n'.join([dict_to_csv(row) for row in rows])
        out.write(csv_data)
        #print(csv_data.encode('utf-8'), file=out)


def json_to_rows(file):
    with open(file) as inp:
        return json.load(inp)['topartists']


csv_file = 'artists.csv'
json_file = 'music_library.json'
#dump_artists_json(json_file, artists_limit=30, country="United States")
#dump_artists_json(json_file, artists_limit=30)
#rows_to_csv(json_to_rows(json_file), csv_file)


def load_json():
    with open(json_file, encoding='utf-8') as json_stream:
        return json.load(json_stream)


class AlbumToDB:
    def __init__(self):
        self.album_counter = 0

    def next(self, db, album, artist_id):
        db.insert(into="Album", row={'Album_Id': self.album_counter, 'Album_Name': album['name'],
                                     'Artist_Id': artist_id, 'Year': album['published']})
        print(self.album_counter)
        self.album_counter += 1


album_to_db = AlbumToDB()


def artist_to_db(db, artist, artist_i):
    albums = artist['albums']
    artist_name = artist['artist']['name']
    db.insert(into="Artist", row={'Artist_Id': artist_i, 'Artist_Name': artist_name,
                                  'Artist_Description': artist['description']})
    for album in albums:
        album_to_db.next(db, album, artist_i)



def json_to_db(db, json_data):
    json_data = json_data['topartists']
    artist_i = 3
    for artist in json_data:
        artist_to_db(db, artist, artist_i)
        artist_i += 1


def main(db):
    #dump_artists_json(json_file, artists_limit=2, country="United States")
    json_data = load_json()
    json_to_db(db, json_data)

    """
    print(db.select_all('Artist'))
    with open(csv_file) as f:
        reader = csv.reader(f, delimiter=';')
        i = 1
        # db.delete_all(entity=Entity.Artist)
        for row in reader:
            # db.insert(into=Entity.Artist, row={Artist.Name: row[0], Artist.Id: i})
            i += 1

    data = db.select_all('Artist')
    data = db.fulltext_search_all_match(entity='Artist', attribute='Artist_Name', key='Nirvana')
    table_wrapper = terminaltables.SingleTable([('Artist', 'Id')] + data)
    print(table_wrapper.table)
    """


if __name__ == '__main__':
    connection_parameters = {
        'user': 'postgres', 'host': 'localhost', 'password': 'py', 'database': 'db1'
    }
    with MusicLibraryDatabase(**connection_parameters) as db_handle:
        main(db_handle)


"""
net = pylast.LastFMNetwork(**api_account_details)
country = 'Russian Federation'
country = 'Ukraine'
limit = 5
artists = net.get_geo_top_artists(country, limit=limit)
#print(artists)


artists_set = set()

artists = {}
artists = net.get_geo_top_artists(country, limit=1000)

print(artists[0][0])
"""
"""
min_play_count_artist = max(pyprind.prog_bar(artists), key=lambda artist: artist.item.get_playcount())
max_song_count = 50
max_div_min = 10
min_play_count = min_play_count_artist.item.get_playcount()
max_play_count = max_div_min * min_play_count

print(lowest_artist.item.name)
# print(artists[artists.__len__()-1].item.name)
"""
"""
for track in net.get_geo_top_tracks(country=country, limit=100):
    artist = track.item.get_artist()
    if lowest_artist.get_playcount() > artist.get_playcount():
        lowest_artist = artist
    artists_set.add(artist.name)
top_artist_set = set([i.item.name for i in net.get_geo_top_artists(country, limit=artists_set.__len__())])
artists_set = top_artist_set.difference(artists_set)
print(artists_set, artists_set.__len__())
"""
"""
for artist in artists:
    top_tracks = artist.item.get_top_tracks(limit=1)
    print(artist.item.get_bio('summary'))
    print("times: %s" % artist.item.get_playcount())
    for track in top_tracks:
        print(track.item)
    print()
"""
"""
tracks = net.get_geo_top_tracks(country, limit=1000)
for track in tracks:
    print(track.item)
"""
"""
for tag in net.get_top_tags(limit=50):
    print(tag.item)
    for track in tag.item.get_top_tracks(limit=100):
        print(track.item)
"""

# response = requests.get(top_albums_url)
# response_json = response.json()
# print([album['name'] for album in response_json['topalbums']['album']])
