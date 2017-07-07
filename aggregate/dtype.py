#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import re
import ast
import operator

import pandas as pd

##__________________________________________________________________||
def keep_dtype(dest, src, columns = None):

    columns = dest.columns if columns is None else columns

    for col in columns:
        if not col in src.columns: continue
        if dest[col].dtype is src[col].dtype: continue
        if src[col].dtype.name == 'category':
            dest[col] = dest[col].astype('category',
                                       categories = src[col].cat.categories,
                                       ordered = src[col].cat.ordered)
            continue
        dest[col] = dest[col].astype(src[col].dtype)

    return dest

##__________________________________________________________________||
def convert_column_types_to_category(tbl, columns):
    tbl = tbl.copy()

    ## http://pandas.pydata.org/pandas-docs/stable/categorical.html
    for c in columns:
        if not c in tbl.columns: continue

        tbl[c] = tbl[:][c].astype('category', ordered = True)
        ## not clear why.  but without '[:]', sometime 'ordered' is not effective

        try:
            ## order numerically if numeric
            categories = [(e, ast.literal_eval(str(e))) for e in tbl[c].cat.categories]
            # e.g., [('100.47', 100.47), ('15.92', 15.92), ('2.0', 2.0)]

            categories = sorted(categories, key = operator.itemgetter(1))
            # e.g., [('2.0', 2.0), ('15.92', 15.92), ('100.47', 100.47)]

            categories = [e[0] for e in categories]
            # e.g., ['2.0', '15.92', '100.47']

        except:
            ## alphanumeric sort
            categories = sorted(tbl[c].cat.categories, key = lambda n: [float(c) if c.isdigit() else c for c in re.findall('\d+|\D+', n)])

        tbl[c].cat.reorder_categories(categories, ordered = True, inplace = True)

    return tbl

##__________________________________________________________________||
def rbind_tbls(tbls):

    ## make a copy so as not to modify the original
    tbls = [t.copy() for t in tbls]

    ## appending categories is a bit odd
    ## (https://github.com/pandas-dev/pandas/issues/12699)
    ## so, keep the list of category columns
    category_columns = set()
    for tbl in tbls:
        category_columns.update([c for c in tbl.columns if tbl[c].dtype.name == 'category'])

    ## then make all categories str
    for tbl in tbls:
        for col in tbl.columns:
            if not tbl[col].dtype.name == 'category': continue
            tbl[col] = tbl[col].astype(str)

    ## keep only the intersection of the columns of all tbls
    columns = set.intersection(*[set(t.columns.values) for t in tbls])
    for tbl in tbls:
        tbl.drop([c for c in tbl.columns if c not in columns], axis = 1, inplace = True)

    ##
    tbl_out = pd.concat(tbls)

    ##
    tbl_out = convert_column_types_to_category(tbl_out, category_columns)

    return tbl_out

##__________________________________________________________________||
