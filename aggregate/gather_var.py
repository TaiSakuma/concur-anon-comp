# Tai Sakuma <tai.sakuma@cern.ch>

##__________________________________________________________________||
def gather_var(tbl, varname):
    tbl.rename(columns = {varname: 'val'}, inplace = True)
    tbl['var'] = varname
    columns = tbl.columns.values.tolist()
    columns.remove('var')
    columns.insert(columns.index('val'), 'var')
    tbl = tbl[columns]
    return tbl

##__________________________________________________________________||
