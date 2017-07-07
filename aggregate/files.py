#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os

import pandas as pd

from .dtype import convert_column_types_to_category

##__________________________________________________________________||
def all_outputs_are_newer_than_any_input(outfile_paths, infile_paths):
    existing_infile_paths = [p for p in infile_paths if os.path.exists(p)]
    for outfile_path in outfile_paths:
        if not os.path.exists(outfile_path): return False
        for infile_path in existing_infile_paths:
            if os.path.getmtime(outfile_path) <= os.path.getmtime(infile_path): return False
    return True

##__________________________________________________________________||
def write_to_file(tbl, path, to_string_option = dict(index = False)):
    print 'writing ', path
    with open(path, 'w') as f:
        tbl.to_string(f, **to_string_option)
        f.write("\n")
        f.close()

##__________________________________________________________________||
def custom_pd_read_table(path, dtype = { }):
    """

    read a data frame from a file with all columns as categories
    unless other types are specified.

    dtype : e.g., {'n': float, 'nvar': float}

    """

    try:
        columns = open(path).readline().split()
    except TypeError: # path is buffer
        columns = path.readline().split()
        path.seek(0)
    # e.g, columns = ['component', 'cutflow', 'bintype', 'nJet100', 'n', 'nvar']

    columns_specified = dtype.keys()
    # e.g. ['nvar', 'n']

    str_columns = [c for c in columns if c not in columns_specified]
    # e.g, ['component', 'cutflow', 'bintype', 'nJet100']

    column_type_dict = dtype.copy()
    column_type_dict.update(dict([(c, str) for c in str_columns]))
    # e.g. {'component': <type 'str'>, 'nJet100': <type 'str'>, 'nvar': <type
    # 'float'>, 'cutflow': <type 'str'>, 'bintype': <type 'str'>, 'n': <type
    # 'float'>}

    tbl = pd.read_table(path, delim_whitespace = True, dtype = column_type_dict)

    return convert_column_types_to_category(tbl, str_columns)

##__________________________________________________________________||
