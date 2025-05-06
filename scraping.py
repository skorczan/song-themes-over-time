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

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_albums_artist_id ON ALBUMS(artist_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_albums_release_date ON ALBUMS(release_date);')

    cursor.execute('''CREATE TABLE IF NOT EXISTS SONGS (
        id INTEGER PRIMARY KEY,
        album_id INTEGER FOREIGNKEY REFERENCES ALBUMS(id),
        release_date TEXT,
        title TEXT,
        lyrics TEXT
    );''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_songs_album_id ON SONGS(album_id);');
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_songs_release_date ON SONGS(release_date);');

    cursor.execute('''CREATE TABLE IF NOT EXISTS SONGS_AUTHORS (
        song_id INTEGER FOREIGNKEY REFERENCES SONG(id) NOT NULL,
        artist_id INTEGER FOREIGNKEY REFERENCES ARTISTS(id) NOT NULL,
        PRIMARY KEY (song_id, artist_id)
    );''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_songs_authors_artist_id ON SONGS_AUTHORS(artist_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_songs_authors_song_id ON SONGS_AUTHORS(song_id);')


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
                                AND NOT EXISTS (SELECT sa.song_id FROM SONGS_AUTHORS sa WHERE sa.artist_id = a.id)
                          ORDER BY RANDOM()
                       ''')

        while True:
            batch = cursor.fetchmany(batch_size)

            if not batch:
                break

            for (artist_id,) in batch:
                populate_songs_by_artist(artist_id, genius, db)

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
            song_rows = []
            song_author_rows = []

            for song in data['songs']:
                id = song['id']

                saved_song = db.execute("SELECT * FROM SONGS WHERE id = ?", (id,)).fetchone()

                if saved_song is None:
                    title = song.get('title', 'UNKNOWN')
                    lyrics = grab_lyrics(song)
                    release_date = format_release_date(song)

                    #db.execute(
                    #    "INSERT INTO SONGS (id, title, release_date, lyrics) VALUES (?, ?, ?, ?)",
                    #    (id, title, release_date, lyrics),
                    #)
                    song_rows.append((id, title, release_date, lyrics))

                #db.execute("INSERT OR IGNORE INTO SONGS_AUTHORS (song_id, artist_id) VALUES (?, ?)", (id, artist_id))
                song_author_rows.append((id, artist_id))

            db.executemany("INSERT INTO SONGS (id, title, release_date, lyrics) VALUES (?, ?, ?, ?)", song_rows)
            db.executemany("INSERT OR IGNORE INTO SONGS_AUTHORS (song_id, artist_id) VALUES (?, ?)", song_author_rows)

            page = data['next_page']

            if page is None:
                break

        db.commit()


def main():
    token = environ.get('GENIUS_TOKEN')

    db = sqlite3.connect("songs.db")
    db.execute("PRAGMA journal_mode = WAL;")
    db.execute("PRAGMA synchronous = NORMAL;")

    ensure_database_is_initialized(db)

    genius = Genius(token, timeout=10)
    # populate_artist_table(genius, db)
    populate_songs_table(genius, db)
    #populate_songs_by_artist(1, genius, db)
    #populate_songs_by_artist(20, genius, db)
    #populate_songs_by_artist(21, genius, db)


if __name__ == '__main__':
    main()
