from ..core import Unit, outport, inport, coroutine, withloop
from ..ext import param, Paramed
from ..flows.tools import Collect

import pandas as pd
import numpy as np
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
    """ converts a list to as seires """
    @outport
    def out(self, data : pd.Series, tag):
        pass

    @inport
    def ins(self, data, tag):
        if not data:
            yield from pd.Series() >> tag >> self.out

        data = pd.Series(data)
        if tag.lineno:
            data.index += tag.lineno
        elif tag.offset:
            data.index += tag.offset
        yield from data >> tag >> self.out

class IndexContinued(Unit):
    @outport
    def out(self, data, tag):
        pass

    @coroutine
    def __setup__(self):
        self.offset = 0

    @inport
    def ins(self, data, tag):
        offset = self.offset
        self.offset = end = offset + len(data)
        data.index = np.arange(offset, end)
        yield from data >> tag >> self.out


class Unstack(Unit):
    @outport
    def out(): pass

    @inport
    def ins(self, data : pd.Series, tag):
        yield from data.unstack() >> tag >> self.out


class Drop(Unit):
    def __init__(self, drop, *args, **kws):
        super().__init__(*args, **kws)
        self.drop = drop

    @outport
    def out(): pass

    @inport
    def ins(self, data: pd.DataFrame, tag):
        data = data[data.columns - self.drop]
        yield from data >> tag >> self.out


class Select(Unit):
    def __init__(self, select, *args, **kws):
        super().__init__(*args, **kws)
        self.select = select

    @outport
    def out(): pass

    @inport
    def ins(self, data: pd.DataFrame, tag):
        data = data[data.columns & self.select]
        yield from data >> tag >> self.out


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
    def ins(self, data : pd.DataFrame, tag):
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
    def ins(self, data : pd.DataFrame, tag):
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
    def ins(self, data : pd.DataFrame, tag):
        yield from data.query(self.query) >> tag >> self.out

