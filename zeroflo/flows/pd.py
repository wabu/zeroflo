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
    def ins(self, data : [...], tag : {'lineno': int}):
        if not data:
            return
        data = pd.Series(data)
        if tag.lineno:
            data.index += tag.lineno
        yield from data >> tag >> self.out


class Unstack(Unit):
    @outport
    def out(): pass

    @inport
    def ins(self, data : pd.Series, tag):
        yield from data.unstack() >> tag >> self.out


class InferTypes(Unit):
    @outport
    def out(): pass

    @staticmethod
    def try_convert(x):
        if not pd.lib.is_string_array(x.values):
            return x
        try:
            x = pd.Series(pd.lib.maybe_convert_numeric(x.values, {'nan', 'NaN'}), 
                    index=x.index)
        except ValueError as e:
            pass
        if (x.dtype == np.dtype(int)
            and ((x > 5e11) & (x < 20e11)).all()):
            x = x.astype('datetime64[ms]')
        return x

    @inport
    def ins(self, data : pd.DataFrame, tag):
        yield from data.apply(self.try_convert, axis=0) >> tag >> self.out


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

