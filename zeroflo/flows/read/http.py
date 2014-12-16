from .ressource import Ressource, Directory, Stats
from ...ext.params import param

from pyadds.annotate import cached, delayed
from pyadds.logging import log

import pandas as pd
import asyncio
import aiohttp
coroutine = asyncio.coroutine

from functools import wraps


@log
class HTTPConnection:
    """ aiohttp based http connection to server """
    def __init__(self, base, connect={}, loop=None, **rqs):
        self.base = base
        self.connect = connect
        if 'auth' in rqs:
            rqs['auth'] = aiohttp.helpers.BasicAuth(*rqs['auth'])
        self.rqs = rqs

    @delayed
    def connector(self):
        return (self.rqs.pop('connector', None)
                or aiohttp.TCPConnector(loop=asyncio.get_event_loop(), **self.connect))

    @cached
    def methods(self):
        return {}

    @coroutine
    def request(self, method, path, **kws):
        kws.update(self.rqs)
        url = '/'.join([self.base, path])
        return (yield from aiohttp.request(method, url, connector=self.connector, **kws))

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError

        try:
            return self.methods[name]
        except KeyError:
            request = self.request
            method = name.upper()
            @coroutine
            @wraps(request)
            def rq(path, **kws):
                return (yield from request(method, path, **kws))
            self.methods[name] = rq
            return rq


@log
class HTTPRessource(Ressource):
    """ ressource access via http """
    def __init__(self, path, conn):
        super().__init__(path)
        self.conn = conn

    @property
    @coroutine
    def stat(self):
        r = yield from self.conn.head(self.path)
        if r.status >= 300:
            return None
        h = r.headers

        return Stats(self.path, self.path.endswith('/'),
                pd.Timestamp(h.get('last-modified', pd.NaT)),
                int(h.get('content-length', -1)))

    @coroutine
    def text(self, encoding=None):
        r = yield from self.conn.get(self.path)
        return (yield from r.text(encoding=encoding))

    @coroutine
    def bytes(self):
        r = yield from self.conn.get(self.path)
        return (yield from r.read())

    @coroutine
    def reader(self, offset=None):
        opts = {}
        if offset:
            opts['headers'] = {'Range': 'bytes=%d-' % offset}
        r = yield from self.conn.get(self.path, **opts)
        self.raise_from_status(r, exspect=206 if offset else 200)
        reader = r.content

        if offset:
            if r.status != 206:
                self.__log.warning('read %s with offset=%d, but no partial response', self.path, offset)
                skip = offset
            else:
                result = int(r.headers['content-range'].split('-',1)[0].rsplit(' ')[-1])
                if result != offset:
                    self.__log.warning('read %s with offset=%d returned foo', self.path, offset)
                    skip = offset-result
                else:
                    skip = 0

            if skip:
                try:
                    yield from reader.readexactly(skip)
                except asyncio.IncompleteReadError as e:
                    raise OSError from e

        return reader

    def raise_from_status(self, r, exspect=200):
        error = None
        if 400 <= r.status < 500:
            error = 'Client'
        elif 500 <= r.status < 600:
            error = 'Server'
        elif r.status >= 600 or r.status < 100:
            error = 'Unexpected'
        elif r.status != exspect:
            self.__log.debug('returning normaly with status %d %s (%d exspected)', r.status, r.reason, exspect)

        if error:
            raise OSError(r.status,
                          '{} {} Error: {}'.format(r.status, error, r.reason),
                          self.path)


@log
class HTTPDirectory(HTTPRessource, Directory):
    """ directory access via http """
    def __init__(self, path, conn, columns=None, **rqs):
        super().__init__(path, conn=conn)
        self.columns = columns
        self.rqs = rqs

    def extract_stat(self, stat):
        return Stats(stat['name'], stat['dir'], pd.Timestamp(stat['modified']), stat['size'])

    @coroutine
    def stats(self, glob=None):
        html = yield from self.text()
        tbl, = pd.read_html(html, **self.rqs)
        if self.columns:
            if isinstance(self.columns, dict):
                tbl = tbl.rename(columns=self.columns)
            else:
                tbl.columns=self.columns

        if 'dir' not in tbl:
            tbl['dir'] = tbl['name'].str.endswith('/')
        if tbl['size'].dtype == object:
            tbl['size'] = tbl['size'].apply(param.sizeof)

        return list(tbl.apply(self.extract_stat, axis=1).values)

    def open(self, name: str):
        return HTTPRessource('/'.join([self.path, name]), conn=self.conn)

    def go(self, name: str):
        return HTTPDirectory('/'.join([self.path, name]), conn=self.conn, columns=self.columns, **self.rqs)

