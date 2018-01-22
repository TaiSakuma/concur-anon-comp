# Tai Sakuma <tai.sakuma@cern.ch>
import sys, os
import pandas as pd

_setup = False

##__________________________________________________________________||
def setup():

    global _setup
    if _setup:
        return

    update_sys_path()
    set_pandas_options()

    _setup = True


##__________________________________________________________________||
def update_sys_path():

    scripts_subdir = os.path.dirname(__file__)
    scripts_dir = os.path.dirname(scripts_subdir)
    sys.path.insert(1, scripts_dir)

    external_dir = os.path.join(scripts_dir, 'external')
    sys.path.insert(1, external_dir)

    alphatwirl_path = os.path.join(external_dir, 'alphatwirl')
    sys.path.insert(1, alphatwirl_path)

##__________________________________________________________________||
def set_pandas_options():

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 4096)
    pd.set_option('display.max_rows', 65536)
    pd.set_option('display.width', 1000)

##__________________________________________________________________||
