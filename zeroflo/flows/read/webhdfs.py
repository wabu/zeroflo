from .ressource import Stats
from .http import HTTPRessource, HTTPDirectory

from pyadds.logging import log

import pandas as pd
import asyncio
coroutine = asyncio.coroutine


@log
class WebHDFSRessource(HTTPRessource):
    """ ressource access via http """
    def __init__(self, path, conn):
        super().__init__(path, conn)

    @property
    @coroutine
    def stat(self):
        r = yield from self.conn.get(self.path,
                                     params={'op': 'GETFILESTATUS'})
        if r.status != 200:
            return None
        stat = (yield from r.json())["FileStatus"]
        yield from r.release()

        return Stats(stat['pathSuffix'], stat['type'] == 'DIRECTORY',
                     pd.Timestamp(stat['modificationTime'], unit='ms'),
                     stat["length"])

    @coroutine
    def text(self, encoding=None):
        r = yield from self.conn.get(self.path, allow_redirects=True,
                                     params={'op': 'OPEN'})
        return (yield from r.text(encoding=encoding))

    @coroutine
    def bytes(self):
        r = yield from self.conn.get(self.path, allow_redirects=True,
                                     params={'op': 'OPEN'})
        return (yield from r.read())

    @coroutine
    def reader(self, offset=None):
        params = {'op': 'OPEN'}
        if offset:
            params['offset'] = offset
        r = yield from self.conn.get(self.path, allow_redirects=True,
                                     params=params)
        self.raise_from_status(r, exspect=200)
        reader = r.content
        return reader


@log
class WebHDFSDirectory(WebHDFSRessource, HTTPDirectory):
    """ directory access via http """
    def __init__(self, path, conn):
        super().__init__(path, conn=conn)

    def extract_stat(self, stat):
        return Stats(stat['pathSufix'], stat['type'] == 'DIRECTORY',
                     pd.Timestamp(stat['modificationTime'], unit='ms'),
                     stat["length"])

    @coroutine
    def stats(self, glob=None):
        r = yield from self.conn.get(self.path, params={'op': 'LISTSTATUS'})
        self.raise_from_status(r, exspect=200)

        lst = (yield from r.json())['FileStatuses']['FileStatus']
        return list(map(self.extract_stat, lst))

    def open(self, name: str):
        return WebHDFSRessource('/'.join([self.path, name]), conn=self.conn)

    def go(self, name: str):
        return WebHDFSDirectory('/'.join([self.path, name]), conn=self.conn)
