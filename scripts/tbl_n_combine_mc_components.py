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
parser.add_argument('dir', help = 'path to the tbl dir')
parser.add_argument("--lumi", action = "store", default = 1, type = float, help = "target luminosity [pb-1]")
parser.add_argument("--dataset-column", default = 'component', help = "the column name for data set")
parser.add_argument("--nevt-column", default = 'nevt_sumw', help = "the column name for nevt")
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
exclude_list = ('tbl_n.component_xsec.txt', ) # because the column name conflicts

##__________________________________________________________________||
def main():

    parallel = fwtwirl.build_parallel(parallel_mode = args.parallel_mode, processes = args.process)
    parallel.begin()

    tbldir = args.dir

    tbl_xsec_file_path = os.path.join(tbldir, 'tbl_xsec.txt')
    tbl_nevt_file_path = os.path.join(tbldir, 'tbl_nevt.txt')
    tbl_component_process_file_path = os.path.join(tbldir, 'tbl_cfg_component_phasespace_process.txt')
    tbl_xsec = agg.custom_pd_read_table(tbl_xsec_file_path, dtype = dict(xsec = float))
    tbl_nevt = agg.custom_pd_read_table(tbl_nevt_file_path, dtype = dict(nevt = float, nevt_sumw = float))
    tbl_component_process = agg.custom_pd_read_table(tbl_component_process_file_path)

    infile_name_regex = r'^tbl_n\.component(\..*)?\.txt$'
    infile_names = [f for f in os.listdir(tbldir) if re.search(infile_name_regex, f)]
    infile_names = [n for n in infile_names if n not in exclude_list]

    for infile_name in infile_names:
        infile_path = os.path.join(tbldir, infile_name)
        if not os.path.exists(infile_path): continue
        outfile_name1 = re.sub(r'^tbl_n\.component([._])', r'tbl_n.process\1', infile_name)
        outfile_path1 = os.path.join(tbldir, outfile_name1)

        if not args.force:
            if agg.all_outputs_are_newer_than_any_input(
                    outfile_paths = (outfile_path1, ),
                    infile_paths = (infile_path, tbl_xsec_file_path, tbl_nevt_file_path, tbl_component_process_file_path)
            ): continue

        parallel.communicationChannel.put(
            job,
            outfile_path1 = outfile_path1,
            infile_path = infile_path,
            tbl_component_process = tbl_component_process,
            tbl_nevt = tbl_nevt,
            tbl_xsec = tbl_xsec,
            lumi = args.lumi,
            dataset_column = args.dataset_column,
            nevt_column = args.nevt_column
        )

    results = parallel.communicationChannel.receive()
    parallel.end()

##__________________________________________________________________||
def job(outfile_path1, infile_path, tbl_component_process, tbl_nevt,
        tbl_xsec, lumi, dataset_column, nevt_column):

    tbl_n_component = agg.custom_pd_read_table(
        infile_path,
        dtype = dict(n = float, nvar = float, luminosity = float)
    )

    if tbl_n_component.empty: return

    tbl_out = agg.combine_mc_components(tbl_n_component,
                                    tbl_component_process, tbl_nevt,
                                    tbl_xsec, lumi, dataset_column,
                                    nevt_column)

    if tbl_out is None: return
    if tbl_out.empty: return

    agg.write_to_file(tbl_out, outfile_path1)

##__________________________________________________________________||
if __name__ == '__main__':
    if args.profile:
        fwtwirl.profile_func(func = main, profile_out_path = args.profile_out_path)
    else:
        main()
