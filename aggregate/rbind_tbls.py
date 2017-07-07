# Tai Sakuma <tai.sakuma@cern.ch>
import pandas as pd

from .dtype import convert_column_types_to_category

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
