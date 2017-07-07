# Tai Sakuma <tai.sakuma@cern.ch>

from .dtype import keep_dtype

##__________________________________________________________________||
def sum_over_categories(tbl, categories, variables):

    if categories is None:
        categories = ()

    categories = tuple(categories)

    variables = tuple(v for v in variables if v in tbl.columns.values)
    # e.g., ('n', 'nvar')

    factor_names = [c for c in tbl.columns if c not in categories + variables]
    # e.g., ['phasespace', 'process', 'htbin', 'njetbin']

    if not factor_names:

        # group by dummy index [1, 1, ...]
        tbl = tbl.groupby([1]*len(tbl.index))[variables].sum().reset_index()

        # remove the column added by groupby, which is 'index' unless
        # 'index' already exists
        tbl = tbl.drop([c for c in tbl.columns if c not in variables], axis = 1)

        return tbl

    ret = tbl.groupby(factor_names)[variables].sum().reset_index().dropna()

    ret = ret.reset_index(drop = True)

    ret = keep_dtype(ret, tbl)

    return ret

##__________________________________________________________________||
