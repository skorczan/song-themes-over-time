import atexit
import signal
import time
from os import environ

from lyricsgenius import Genius
import sqlite3
import numpy as np
import requests
import threading
import queue


batch_size = 100

stop_event = threading.Event()

class DatabaseReader:

    def __init__(self, db: sqlite3.Connection):
        self._db  = db

    def has_artist(self, artist_id):
        return self._db.execute("SELECT id FROM ARTISTS WHERE id = ?", (artist_id,)).fetchone() is not None

    def has_song(self, song_id):
        return self._db.execute("SELECT id FROM SONGS WHERE id = ?", (song_id,)).fetchone() is not None

    def has_song_lyrics(self, song_id):
        return self._db.execute("SELECT lyrics FROM SONGS WHERE id = ?", (song_id,)).fetchone() is not None


class DatabaseWriter:

    def __init__(self, db: sqlite3.Connection):
        super().__init__()

        self._db = db
        self._lock = threading.RLock()
        self._artists = []
        self._songs = []
        self._lyrics = []
        self._songs_authors = []

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()
        return self

    def keep_artist(self, id, name, followers_count):
        with self._lock:
            print("adding artist", id)
            self._artists.append((id, name, followers_count))

    def keep_song(self, id, title, release_date):
        with self._lock:
            print("adding song", id)
            self._songs.append((id, title, release_date))

    def keep_song_author(self, song_id, artist_id):
        with self._lock:
            self._songs_authors.append((song_id, artist_id))

    def keep_lyrics(self, song_id, text):
        with self._lock:
            print("adding lyrics", song_id)
            self._lyrics.append((song_id, text))

    @property
    def dirty(self):
        return self._artists or self._songs or self._lyrics or self._songs_authors

    def flush(self):
        with self._lock:
            if not self.dirty:
                print("not flushing because i'm clean")
                return

            artists = self._artists
            self._artists = []

            songs = self._songs
            self._songs = []

            songs_authors = self._songs_authors
            self._songs_authors = []

            lyrics = self._lyrics
            self._lyrics = []

        self._db.execute("BEGIN")

        try:
            if artists:
                self._db.executemany(
                    "INSERT OR IGNORE INTO ARTISTS (id, name, followers_count) VALUES (?, ?, ?)", artists)

            if songs:
                self._db.executemany("INSERT OR IGNORE INTO SONGS (id, title, release_date) VALUES (?, ?, ?)", songs)

            if lyrics:
                for (song_id, text) in lyrics:
                    self._db.execute("UPDATE SONGS SET lyrics = ? WHERE id = ?", (text, song_id))

            if songs_authors:
                self._db.executemany("INSERT OR IGNORE INTO SONGS_AUTHORS (song_id, artist_id) VALUES (?, ?)", songs_authors)

            self._db.execute("COMMIT")
            print("Artists saved:", len(artists))
            print("Songs saved:", len(songs))
            print("Lyrics saved:", len(lyrics))
            print("Song authors saved:", len(songs_authors))
        except:
            self._db.execute("ROLLBACK")

            with self._lock:
                self._artists = artists + self._artists
                self._songs = songs + self._songs
                self._songs_authors = songs_authors + self._songs_authors
                self._lyrics = lyrics + self._lyrics

            raise


class DataKeeper(threading.Thread, DatabaseWriter):

    def __init__(self):
        threading.Thread.__init__(self)
        DatabaseWriter.__init__(self, None)
        self.name = "DataKeeper"

    def run(self):
        self._db = open_database_connection()

        try:
            time.sleep(10)

            while not stop_event.is_set():
                self.flush()
                time.sleep(10)
        finally:
            self._db.close()


class ArtistScrapper(threading.Thread):

    def __init__(self, source: queue.Queue, database: DatabaseWriter):
        super().__init__()

        self.name = "ArtistScraper"
        self._source = source
        self._db = database

    def run(self):
        genius = genius_api()

        while not stop_event.is_set():
            try:
                artist_id = self._source.get(timeout=60)

                try:
                    data = genius.artist(artist_id)
                    artist = data['artist']
                except requests.exceptions.RequestException as e:
                    artist = {'id': int(artist_id), 'name': str(e.errno), 'followers_count': -1}

                self._db.keep_artist(artist['id'], artist.get('name', 'UNKNOWN'), int(artist.get('followers_count', 0)))
                self._source.task_done()
            except queue.Empty:
                continue
            except queue.ShutDown:
                break


class SongsScrapper(threading.Thread):

    def __init__(self, source: queue.Queue, target: queue.Queue, db_writer: DatabaseWriter):
        super().__init__()

        self.name = "SongsScraper"
        self._source = source
        self._target = target
        self._db_writer = db_writer

    def run(self):
        genius = genius_api()

        while not stop_event.is_set():
            try:
                artist_id = self._source.get(timeout=60)

                with open_database_connection() as db:
                    db_reader = DatabaseReader(db)

                    page = 1

                    while page is not None:
                        data = genius.artist_songs(artist_id, page=page)

                        for song in data['songs']:
                            song_id = song['id']

                            if not db_reader.has_song(song_id):
                                title = song.get('title', 'UNKNOWN')
                                release_date = self._format_release_date(song)

                                self._db_writer.keep_song(song_id, title, release_date)

                            self._db_writer.keep_song_author(song_id, artist_id)
                            self._target.put(song_id)

                        page = data['next_page']

                    self._source.task_done()
            except queue.Empty:
                continue
            except queue.ShutDown:
                break

    def _format_release_date(self, song):
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

class SongLyricsScrapper(threading.Thread):

    def __init__(self, source: queue.Queue, db: DatabaseWriter):
        super().__init__()

        self.name = "SongLyricsScraper"
        self._source = source
        self._db = db

    def run(self):
        genius = genius_api()

        while not stop_event.is_set():
            try:
                song_id = self._source.get(timeout=60)

                try:
                    lyrics = genius.lyrics(song_id, remove_section_headers=True)
                except requests.exceptions.RequestException as e:
                    lyrics = e.strerror

                if lyrics is not None:
                    self._db.keep_lyrics(song_id, lyrics)

                self._source.task_done()
                print("lyrics for song scrapped", song_id)
            except queue.Empty:
                continue


class DataSeeder(threading.Thread):

    def __init__(self, artist_queue: queue.Queue, song_queue: queue.Queue, lyrics_queue: queue.Queue):
        super().__init__()

        self.name = "DataSeeder"
        self._artist_queue = artist_queue
        self._song_queue = song_queue
        self._lyrics_queue = lyrics_queue

    def run(self):
        with open_database_connection() as db:
            ensure_database_is_initialized(db)

            try:
                self._seed_artists(db)
                self._fillup_missing_song_lyrics(db)
                self._fillup_missing_songs_for_interesting_artists(db)
            except queue.Full:
                pass
            except queue.ShutDown:
                pass

    def _seed_artists(self, db: sqlite3.Connection):
        min_artist_id, max_artist_id = 1, 79_999
        artists_seen = np.full(shape=max_artist_id + 1, fill_value=False, dtype=bool)
        artists_seen[0] = True

        for (artist_id,) in db.execute('SELECT id FROM ARTISTS'):
            if min_artist_id <= artist_id <= max_artist_id:
                artists_seen[artist_id] = True
            else:
                raise IndexError(f'Artist ID {artist_id} out of range')

        artists_unseen = np.flatnonzero(~artists_seen)
        np.random.shuffle(artists_unseen)

        for artist_id in artists_unseen:
            if not stop_event.is_set():
                self._artist_queue.put(artist_id)
                time.sleep(0.25)

    def _fillup_missing_song_lyrics(self, db: sqlite3.Connection):
        for (song_id,) in db.execute('SELECT id FROM SONGS WHERE lyrics IS NULL'):
            if not stop_event.is_set():
                self._lyrics_queue.put(song_id)
                time.sleep(0.1)

    def _fillup_missing_songs_for_interesting_artists(self, db: sqlite3.Connection):
        for (artist_id,) in db.execute('''SELECT a.id
                                          FROM ARTISTS a
                                          WHERE a.followers_count >= 50
                                            AND NOT EXISTS (SELECT sa.song_id FROM SONGS_AUTHORS sa WHERE sa.artist_id = a.id)
                                          ORDER BY RANDOM()
                                       '''):
            if not stop_event.is_set():
                self._song_queue.put(artist_id)
                time.sleep(0.25)


def main():
    genius = genius_api()
    del genius

    artist_queue = queue.Queue(maxsize=5)
    songs_queue = queue.Queue(maxsize=10)
    lyrics_queue = queue.Queue(maxsize=100)

    data_seeder = DataSeeder(artist_queue, songs_queue, lyrics_queue)
    data_seeder.start()

    data_keeper = DataKeeper()
    data_keeper.start()

    artist_scrapper = ArtistScrapper(artist_queue, data_keeper)
    song_scrapper = SongsScrapper(songs_queue, lyrics_queue, data_keeper)
    lyrics_scrappers = [ SongLyricsScrapper(lyrics_queue, data_keeper) for _ in range(3) ]
    all_scrappers = [ artist_scrapper, song_scrapper ] + lyrics_scrappers

    def stop():
        stop_event.set()
        data_seeder.join(timeout=60)

        for thread in all_scrappers:
            thread.join(timeout=60)

        data_keeper.flush()
        data_keeper.join(timeout=60)

    signal.signal(signal.SIGINT, lambda sig, frame: stop())
    signal.signal(signal.SIGTERM, lambda sig, frame: stop())
    atexit.register(stop)

    for scrapper in all_scrappers:
        scrapper.start()

    while not stop_event.is_set():
        time.sleep(1)
        print("artist queue:", artist_queue.qsize())
        print("song queue:", songs_queue.qsize())
        print("lyrics queue:", lyrics_queue.qsize())


def open_database_connection():
    db = sqlite3.connect("songs.db")
    db.execute("PRAGMA journal_mode = WAL;")
    db.execute("PRAGMA synchronous = NORMAL;")
    db.execute("PRAGMA busy_timeout = 60000;")
    db.execute("PRAGMA foreign_keys = ON;")
    return db

def genius_api():
    token = environ.get('GENIUS_TOKEN')
    return Genius(token, timeout=10)

def ensure_database_is_initialized(db):
    db.execute('''CREATE TABLE IF NOT EXISTS ARTISTS (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        followers_count INTEGER NOT NULL
    );''')

    db.execute('''CREATE TABLE IF NOT EXISTS ALBUMS (
        id INTEGER PRIMARY KEY,
        artist_id INTEGER REFERENCES ARTISTS(id),
        release_date TEXT,
        title TEXT
    );''')

    db.execute('CREATE INDEX IF NOT EXISTS idx_albums_artist_id ON ALBUMS(artist_id);')
    db.execute('CREATE INDEX IF NOT EXISTS idx_albums_release_date ON ALBUMS(release_date);')

    db.execute('''CREATE TABLE IF NOT EXISTS SONGS (
        id INTEGER PRIMARY KEY,
        album_id INTEGER REFERENCES ALBUMS(id),
        release_date TEXT,
        title TEXT,
        lyrics TEXT
    );''')

    db.execute('CREATE INDEX IF NOT EXISTS idx_songs_album_id ON SONGS(album_id);')
    db.execute('CREATE INDEX IF NOT EXISTS idx_songs_release_date ON SONGS(release_date);')

    db.execute('''CREATE TABLE IF NOT EXISTS SONGS_AUTHORS (
        song_id INTEGER REFERENCES SONG(id) NOT NULL,
        artist_id INTEGER REFERENCES ARTISTS(id) NOT NULL,
        PRIMARY KEY (song_id, artist_id)
    );''')

    db.execute('CREATE INDEX IF NOT EXISTS idx_songs_authors_artist_id ON SONGS_AUTHORS(artist_id);')
    db.execute('CREATE INDEX IF NOT EXISTS idx_songs_authors_song_id ON SONGS_AUTHORS(song_id);')


if __name__ == '__main__':
    main()
