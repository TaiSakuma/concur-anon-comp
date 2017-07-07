#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os, sys
import re
import argparse
import logging

##__________________________________________________________________||
from setup import setup
setup()

##__________________________________________________________________||
import aggregate as agg
import fwtwirl

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('--dir', help = 'path to the tbl dir')
parser.add_argument('--infile-name-prefix', default = 'tbl_n\.process', help = "the prefix of the input file names")
parser.add_argument('--outfile-name-prefix', default = 'tbl_n.process', help = "the prefix of the output file names")
parser.add_argument('--infile-categories', default = [ ], nargs = '*', help = "categories in input files")
parser.add_argument('--categories', default = [ ], nargs = '*', help = "categories to sum over")
parser.add_argument("--force", action = "store_true", default = False, dest="force", help = "recreate all output files")
parser.add_argument('--parallel-mode', default = 'multiprocessing', choices = ['multiprocessing', 'subprocess', 'htcondor'], help = "mode for concurrency")
parser.add_argument("-p", "--process", default = 8, type = int, help = "number of processes to run in parallel")
parser.add_argument('--profile', action = 'store_true', help = 'run profile')
parser.add_argument('--profile-out-path', default = None, help = 'path to write the result of profile')
parser.add_argument('--logging-level', default = 'WARN', choices = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help = 'level for logging')
args = parser.parse_args()

##__________________________________________________________________||
logging.basicConfig(level = logging.getLevelName(args.logging_level))

##__________________________________________________________________||
exclude_list = [ ]

##__________________________________________________________________||
def main():

    parallel = fwtwirl.build_parallel(parallel_mode = args.parallel_mode, processes = args.process)
    parallel.begin()

    tbldir = args.dir

    # e.g.,
    # args.infile_categories = ['htbin', 'njetbin', 'mhtbin']
    # args.categories = ['mhtbin']
    # args.infile_name_prefix = 'tbl_n_process'
    # args.outfile_name_prefix = 'tbl_n_process'

    infile_name_categories = '.'.join(args.infile_categories)
    if infile_name_categories: infile_name_categories = '.' + infile_name_categories
    # e.g., '.htbin.njetbin.mhtbin'

    outfile_categories = [c for c in args.infile_categories if c not in args.categories]
    # e.g., ['htbin', 'njetbin']

    outfile_name_categories = '.'.join(outfile_categories)
    if outfile_name_categories: outfile_name_categories = '.' + outfile_name_categories
    # e.g., '.htbin.njetbin'

    infile_name_regex = r'^{}{}(\..*)?\.txt$'.format(args.infile_name_prefix, infile_name_categories)
    # e.g., '^tbl_n_process.htbin.njetbin.mhtbin(\..*)?\.txt$'

    infile_names = [f for f in sorted(os.listdir(tbldir)) if re.search(infile_name_regex, f)]
    # e.g., ['tbl_n_process.htbin.njetbin.mhtbin.minChi.txt', 'tbl_n_process.htbin.njetbin.mhtbin.minbDphi.txt']

    for infile_name in infile_names:

        # e.g., infile_name = 'tbl_n_process.htbin.njetbin.mhtbin.minChi.txt'

        infile_path = os.path.join(tbldir, infile_name)
        if not os.path.exists(infile_path): continue

        outfile_name1 = re.sub(
            r'^{}{}([._])'.format(args.infile_name_prefix, infile_name_categories),
            r'{}{}\1'.format(args.outfile_name_prefix, outfile_name_categories),
            infile_name)
        # e.g., 'tbl_n_process.htbin.njetbin.minChi.txt'

        outfile_path1 = os.path.join(tbldir, outfile_name1)

        if not args.force:
            if agg.all_outputs_are_newer_than_any_input(
                    outfile_paths = (outfile_path1, ),
                    infile_paths = (infile_path, )
            ): continue

        parallel.communicationChannel.put(job, outfile_path1, infile_path, args.categories)

    results = parallel.communicationChannel.receive()
    parallel.end()

##__________________________________________________________________||
def job(outfile_path1, infile_path, categories):

    tbl_n_categories = agg.custom_pd_read_table(
        infile_path, dtype = dict(n = float, nvar = float)
    )

    if tbl_n_categories.empty: return

    tbl_out = agg.sum_over_categories(
        tbl_n_categories,
        categories = tuple(categories),
        variables = ('n', 'nvar')
    )

    if tbl_out is None: return
    if tbl_out.empty: return

    agg.write_to_file(tbl_out, outfile_path1)

##__________________________________________________________________||
if __name__ == '__main__':
    if args.profile:
        fwtwirl.profile_func(func = main, profile_out_path = args.profile_out_path)
    else:
        main()
