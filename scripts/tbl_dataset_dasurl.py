#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os
import sys
import argparse
import logging

import pandas as pd

##__________________________________________________________________||
from setup import setup
setup()

##__________________________________________________________________||
import aggregate as agg

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument("--force", action = "store_true", default = False, dest="force", help = "recreate all output files")
parser.add_argument('args', nargs=argparse.REMAINDER)
parser.add_argument('--logging-level', default = 'WARN', choices = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help = 'level for logging')
args = parser.parse_args()
tbldirs = args.args

##__________________________________________________________________||
logging.basicConfig(level = logging.getLevelName(args.logging_level))

##__________________________________________________________________||
def main():

    das_address = 'https://cmsweb.cern.ch/das/request'
    dbs_instance = 'prod/global'

    get_query_string = '?view=list' + '&instance=' + dbs_instance.replace('/', '%2F')
    url_pre = das_address + get_query_string + '&input='

    for tbldir in tbldirs:

        outFileName = 'tbl_dataset_dasurl.txt'
        outFilePath = os.path.join(tbldir, outFileName)

        inFileNames = ('tbl_dataset.txt', )
        inFilePaths = [os.path.join(tbldir, n) for n in inFileNames]

        nonexistentInFilePaths = [p for p in inFilePaths if not os.path.exists(p)]
        if not len(nonexistentInFilePaths) == 0:
            sys.stderr.write('file does not exist: ')
            sys.stderr.write(",".join(nonexistentInFilePaths))
            sys.stderr.write('\n')
            return

        if not args.force:
            if agg.all_outputs_are_newer_than_any_input(
                    outfile_paths = [outFilePath],
                    infile_paths = inFilePaths
            ): continue

        tbl_dataset = pd.read_table(os.path.join(tbldir, 'tbl_dataset.txt'), delim_whitespace = True, dtype = str)

        tbl_dataset['dasurl'] = tbl_dataset['dataset'].map(lambda e: url_pre + e.replace('/', '%2F'))

        agg.write_to_file(tbl_dataset, outFilePath)

##__________________________________________________________________||
if __name__ == '__main__':
    main()
