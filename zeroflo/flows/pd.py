from ..core import Unit, outport, inport, coroutine
from ..ext import param, Paramed
from ..flows.tools import Collect

from pyadds.logging import log

import pandas as pd
import numpy as np
import re
import io

from functools import wraps

def skip_empty(port):
    @wraps(port)
    def skipping(self, data, tag):
        if data.empty:
            return
        else:
            yield from port(self, data, tag)
    return skipping


class ToSeries(Unit):
    """ converts a list to a seires """
    @outport
    def out(self, data : pd.Series, tag):
        pass

    @inport
    def process(self, data, tag):
        if not data:
            yield from pd.Series() >> tag >> self.out
            return
        data = pd.Series(data)
        df.index += tag.offset or 0
        yield from data >> tag >> self.out


class ToFrame(Paramed, Unit):
    @param
    def columns(self, val=None):
        return val

    @param
    def dtypes(self, val={}):
        return val

    @param
    def na_values(self, val=''):
        if not isinstance(val, list):
            val = [val]
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        df = pd.DataFrame(data or None, columns=self.columns)
        if not df.empty:
            df.index += tag.offset or 0
            df[df.applymap(self.na_values.__contains__)] = np.nan
            for col,dtype in self.dtypes.items():
                df[col] = df[col].astype(dtype)

        yield from df >> tag.add(size=len(df)) >> self.out


class ToTable(Unit):
    """ converts raw data (bytes) with read_table """
    def __init__(self, **opts):
        super().__init__()
        self.opts = opts

    @outport
    def out():
        """ port for framed data """

    @coroutine
    def __setup__(self):
        self.offset = 0 

    @inport
    def process(self, data, tag):
        """ port getting bytes data """
        df = pd.read_table(io.BytesIO(data), **self.opts)
        length = len(df)
        if not df.empty:
            df.index += self.offset
        self.offset += length
        yield from df >> tag.add(length=length) >> self.out


class DumpTable(Unit):
    def __init__(self, **opts):
        super().__init__()
        self.opts = opts

    @outport
    def out():
        """ port for framed data """

    @coroutine
    def __setup__(self):
        self.header = self.opts.pop('header', False)

    @inport
    def process(self, data, tag):
        """ port getting bytes data """
        out = io.StringIO()
        data.to_csv(out, header=self.header, **self.opts)
        if self.header:
            self.header=False

        yield from out.getvalue().encode() >> tag >> self.out


class Fill(Paramed, Unit):
    @param
    def fills(self, val={}):
        return val

    @outport 
    def out(): pass

    @inport
    def process(self, data, tag):
        for col, fill in self.fills.items():
            data.loc[~data[col].apply(bool),col] = fill

        yield from data >> tag >> self.out


class Convert(Paramed, Unit):
    @param
    def convs(self, val={}):
        return {col: conv if isinstance(conv, list) else [conv] for col, conv in val.items()}

    @outport 
    def out(): pass

    @inport
    def process(self, data, tag):
        data.is_copy = None
        for col, convs in self.convs.items():
            sel = data[col]
            for conv in convs:
                sel = sel.astype(conv)
            data[col] = sel
        yield from data >> tag >> self.out


@log
class Sort(Paramed, Unit):
    @param
    def by(self, val=None):
        return val

    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.buf = []
        self.tag = None

    @coroutine
    def flush(self):
        new = pd.concat(self.buf)
        self.__log.debug('flushing out %d lines', len(new))
        yield from new >> self.tag.add(sorted=True) >> self.out
        self.buf = []

    @inport
    def process(self, data, tag):
        if tag.sorted:
            if self.buf:
                yield from self.flush()
            yield from data >> tag >> self.out
            return

        self.buf.append(data)
        self.tag = tag
        self.__log.debug('adding %d lines -> %d', len(data), len(self.buf))

        if tag.flush:
            yield from self.flush()


class IndexContinued(Unit):
    @outport
    def out(self, data, tag):
        pass

    @coroutine
    def __setup__(self):
        self.offset = 0

    @inport
    def process(self, data, tag):
        offset = self.offset
        self.offset = end = offset + len(data)
        data.index = np.arange(offset, end)
        yield from data >> tag >> self.out


class Unstack(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, data : pd.Series, tag):
        yield from data.unstack() >> tag >> self.out


class Columns(Paramed, Unit):
    def __init__(self, select, *args, **kws):
        super().__init__(*args, **kws)
        self.select = select

    @param
    def drop(self, val=None):
        if val is not None:
            val = pd.Index(val)
        return val

    @param
    def select(self, val=None):
        if val is not None:
            val = pd.Index(val)
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data: pd.DataFrame, tag):
        cols = data.columns
        if self.drop is not None:
            cols = cols.difference(self.drop)
        if self.select is not None:
            cols = cols.intersection(self.select)

        yield from data[cols] >> tag >> self.out


fill_dtypes = {
    np.dtype(bool): False,
    np.dtype(int): 0,
}

class InferTypes(Unit):
    @outport
    def out(): pass

    @staticmethod
    def try_convert(x):
        dn = x.dropna().astype(str)

        # convert user ids to int
        dn = dn[dn!='']
        try:
            if (dn.str[:1]=='u').all():
                dn = dn.str[1:].astype(int)
                x = dn.reindex(x.index, fill_value=0)
        except ValueError:
            pass

        conv = []
        for convert,*args in [
                (pd.lib.maybe_convert_numeric, {'nan', 'NaN'}),
                (pd.lib.try_parse_dates,),
                (pd.lib.maybe_convert_bool,),
            ]:
            try:
                conv = convert(dn.values, *args)
                if conv.dtype != dn.dtype:
                    break
            except ValueError as e:
                pass

        if len(conv):
            dn = pd.Series(conv, dn.index)
            x = pd.Series(conv, index=dn.index).reindex(index=x.index, 
                    fill_value=fill_dtypes.get(conv.dtype, np.nan))

        if (pd.lib.is_integer_array(x.values) or 
            pd.lib.is_float_array(x.values) and (dn.astype(int) == dn).all()):

            dates = dn[dn!=0]
            if len(dates) and ((dates > 5e11) & (dates < 20e11)).all():
                dn = dates.astype('datetime64[ms]')
                x = dn.reindex(x.index)

        return x

    @inport
    def process(self, data : pd.DataFrame, tag):
        data = data.apply(self.try_convert, axis=0)
        yield from data >> tag >> self.out


class CollectPd(Collect):
    def reduce(self, datas):
        return pd.concat(datas)


class Grouper(Paramed, Unit):
    @param
    def groupby(self, column):
        return column

    @outport
    def out(self, data : pd.DataFrame, tag : {'group': ...}):
        pass

    @inport
    def process(self, data : pd.DataFrame, tag):
        for grp, vals in data.groupby(self.groupby, sort=False):
            yield from vals >> tag.add(group=grp) >> self.out


class Query(Paramed, Unit):
    @param
    def query(self, query):
        return query

    @outport
    def out(self, data : pd.DataFrame, tag):
        pass

    @inport
    def process(self, data : pd.DataFrame, tag):
        yield from data.query(self.query) >> tag >> self.out


class Filter(Unit):
    def __init__(self, **filters):
        super().__init__()
        self.filters = filters

    @outport
    def out(self, data : pd.DataFrame, tag):
        pass

    @inport
    def process(self, data : pd.DataFrame, tag):
        for key,filt in self.filters.items():
            data = data[data[key].str.contains(filt)]
            
        yield from data >> tag >> self.out
