#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os
import sys
import argparse
import textwrap
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

##__________________________________________________________________||
logging.basicConfig(level = logging.getLevelName(args.logging_level))

##__________________________________________________________________||
tbldirs = args.args

##__________________________________________________________________||
def main():

    for tbldir in tbldirs:

        outFileName = 'dataset_dasurl.html'
        outFilePath = os.path.join(tbldir, outFileName)

        inFileNames = ('tbl_dataset_dasurl.txt', )
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

        tbl_in = pd.read_table(os.path.join(tbldir, 'tbl_dataset_dasurl.txt'), delim_whitespace = True, dtype = str)

        print 'writing ', outFilePath
        f = open(outFilePath, 'w')

        for i, row in tbl_in.iterrows():
            text = """<a href="{href}" target="_blank"><i class="fa fa-database fa-lg"></i></a> <a href="{href}" target="_blank">{display}</a>
            """.format(href = row['dasurl'], display = row['dataset'])
            f.write(textwrap.dedent(text))

##__________________________________________________________________||
if __name__ == '__main__':
    main()
