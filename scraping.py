from os import environ
from lyricsgenius import Genius
import sqlite3
import numpy as np
import requests
import time

batch_size = 100


def ensure_database_is_initialized(db):
    cursor = db.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS ARTISTS (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        followers_count INTEGER NOT NULL
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS ALBUMS (
        id INTEGER PRIMARY KEY,
        artist_id INTEGER FOREIGNKEY REFERENCES ARTISTS(id),
        release_date TEXT,
        title TEXT
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS SONGS (
        id INTEGER PRIMARY KEY,
        artist_id INTEGER FOREIGNKEY REFERENCES ARTISTS(id),
        album_id INTEGER FOREIGNKEY REFERENCES ALBUMS(id),
        release_date TEXT,
        title TEXT,
        lyrics TEXT
    );''')


def populate_artist_table(genius, db):
    min_artist_id, max_artist_id = 1, 79_999
    artists_seen = np.full(shape=max_artist_id + 1, fill_value=False, dtype=bool)

    with db:
        cursor = db.cursor()
        cursor.execute('SELECT id FROM ARTISTS')

        while True:
            batch = cursor.fetchmany(batch_size)

            if not batch:
                break

            for (artist_id,) in batch:
                if min_artist_id <= artist_id <= max_artist_id:
                    artists_seen[artist_id] = True
                else:
                    raise IndexError('Artist ID ' + artist_id + ' out of range')

        artists_unseen = np.flatnonzero(~artists_seen)
        np.random.shuffle(artists_unseen)

        i = 0
        for artist_id in artists_unseen:
            try:
                data = genius.artist(artist_id)
                artist = data['artist']
            except requests.exceptions.RequestException as e:
                artist = { 'id': int(artist_id), 'name': str(e.errno), 'followers_count': -1 }

            db.execute(
                "INSERT INTO ARTISTS (id, name, followers_count) VALUES (?, ?, ?)",
                (artist['id'], artist.get('name', 'UNKNOWN'), int(artist.get('followers_count', 0)))
            )
            i += 1

            if i % 10 == 0:
                db.commit()


def populate_songs_table(genius, db):
    with db:
        cursor = db.cursor()
        cursor.execute('''SELECT a.id
                          FROM ARTISTS a
                          WHERE a.followers_count >= 50
                                AND NOT EXISTS (SELECT s.id FROM SONGS s WHERE s.artist_id = a.id)
                       ''')

        while True:
            batch = cursor.fetchmany(batch_size)

            if not batch:
                break

            for (artist_id,) in batch:
                i = 0

                while i < 5:
                    try:
                        populate_songs_by_artist(artist_id, genius, db)
                        break
                    except Exception as e:
                        i += 1
                        time.sleep(i)

def populate_songs_by_artist(artist_id, genius, db):

    def format_release_date(song):
        release_date = song['release_date_components']

        if release_date is not None:
            year = release_date['year'] or 0
            month = release_date['month'] or 0
            day = release_date['day'] or 0

            if year is None:
                return None

            if month is None:
                return f'{year:04d}'

            if day is not None:
                return f'{year:04d}-{month:02d}-{day:02d}'
            else:
                return f'{year:04d}-{month:02d}'
        else:
            return None

    def grab_lyrics(song):
        try:
            return genius.lyrics(song['id'], remove_section_headers=True)
        except requests.exceptions.RequestException as e:
            return e.strerror

    page = None

    with db:
        while True:
            data = genius.artist_songs(artist_id, page=page)

            for song in data['songs']:
                id = song['id']
                title = song.get('title', 'UNKNOWN')
                lyrics = grab_lyrics(song)
                release_date = format_release_date(song)

                saved_song = db.execute("SELECT * FROM SONGS WHERE id = ?", (id,)).fetchone()

                if saved_song is None:
                    db.execute(
                        "INSERT INTO SONGS (id, artist_id, title, release_date, lyrics) VALUES (?, ?, ?, ?, ?)",
                        (id, artist_id, title, release_date, lyrics),
                    )
                else:
                    print(f'Song {id} already exists')
                    print(f'Artist id = {song['artist_id']}')
                    print(f'Release date = {song["release_date"]}')
                    print(f'Title = {song["title"]}')
                    print('Trying to save')
                    print(f'Artist id = {artist_id}')
                    print(f'Release date = {release_date}')
                    print(f'Title = {title}')

            page = data['next_page']

            if page is None:
                break
        db.commit()


def main():
    token = environ.get('GENIUS_TOKEN')
    db = sqlite3.connect("songs.db")

    ensure_database_is_initialized(db)

    genius = Genius(token, timeout=10)
    # populate_artist_table(genius, db)
    # populate_songs_table(genius, db)
    populate_songs_by_artist(20, genius, db)
    populate_songs_by_artist(21, genius, db)


if __name__ == '__main__':
    main()
