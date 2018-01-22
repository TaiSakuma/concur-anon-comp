#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>

import os, sys
import itertools
import collections

##__________________________________________________________________||
scripts_subdir = os.path.dirname(__file__)
scripts_dir = os.path.dirname(scripts_subdir)

##__________________________________________________________________||
sys.path.insert(1, scripts_dir)
from command_composer import *
from command_composer_local import *

##__________________________________________________________________||
twirl_option_common = ' '.join([
    '--parallel-mode htcondor',
    '--logging-level INFO'
    ])

##__________________________________________________________________||
heppy_topdir = os.path.join(os.path.sep, 'hdfs', 'SUSY', 'RA1')
# heppy_topdir = os.path.join(os.path.sep, 'Users' ,'sakuma', 'work', 'cms', 'c150130_RA1_data')

##__________________________________________________________________||
tbl_topdir = os.path.join('.', 'tbl_20180122_01')

##__________________________________________________________________||
lumi = 35900 # [pb-1]

##__________________________________________________________________||
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('jobs', nargs = argparse.REMAINDER, help = "names of jobs to be run")
parser.add_argument('--print-jobs', action = 'store_true', default = False, help = 'print the names of all jobs and exit')
args = parser.parse_args()

##__________________________________________________________________||
def main():

    job_dict = build_jobs()

    all_job_names = job_dict.keys()

    job_names = args.jobs if args.jobs else all_job_names

    if args.print_jobs:
        print '\n'.join(all_job_names)
        return

    jobs = [job_dict[j] for j in job_names]

    commands = list(itertools.chain(*[j() for j in jobs]))

    for command in commands:
        print command

##__________________________________________________________________||
def build_jobs():
    ret = collections.OrderedDict()

    ret.update(build_jobs_twirl_heppy_SM())

    ret.update(build_jobs_tbl_process_heppy_SM())

    return ret

##__________________________________________________________________||
def build_jobs_twirl_heppy_SM():

    ret = collections.OrderedDict()

    name = 'SM'

    tbl_dir = os.path.join(tbl_topdir, name)
    heppy_dir = os.path.join(heppy_topdir, '80X', 'MC', '20161021_B03', 'ROC_MC_SM')
    heppy_components = [ ]
    twirl_option = "{common} --max-events-per-process 500000 --mc".format(common = twirl_option_common)

    jobs = build_jobs_twirl_heppy_template(
        name = name,
        tbl_dir = tbl_dir,
        heppy_dir = heppy_dir,
        heppy_components = heppy_components,
        twirl_option = twirl_option
    )
    ret.update(jobs)

    return ret

##__________________________________________________________________||
def build_jobs_twirl_heppy_template(name, tbl_dir, heppy_dir,
                                    heppy_components, twirl_option):

    ret = collections.OrderedDict()

    job = EchoCommands(
        commands = [
            'mkdir -p {name}'.format(name = tbl_dir),
            '{script} --components {components} -i {heppy_dir} -o {tbl_dir} {options}'.format(
                script = os.path.join(scripts_subdir, 'twirl_mktbl_heppy.py'),
                components = ' '.join(heppy_components),
                heppy_dir = heppy_dir,
                tbl_dir = tbl_dir,
                options = twirl_option,
            ),
            '{script} {tbl_dir}'.format(
                script = os.path.join(scripts_dir, 'scripts', 'tbl_dataset_dasurl.py'),
                tbl_dir = tbl_dir
            ),
            '{script} {tbl_dir}'.format(
                script = os.path.join(scripts_dir, 'scripts', 'create_dataset_dasurl_html.py'),
                tbl_dir = tbl_dir
            )
        ]
    )
    ret['summarize_trees_{}'.format(name)] = job

    return ret

##__________________________________________________________________||
def build_jobs_tbl_process_heppy_SM():

    ret = collections.OrderedDict()

    name = 'SM'

    tbl_dir = os.path.join(tbl_topdir, name)
    tbl_cfg_process = os.path.join(scripts_subdir, 'tbl', 'tbl_cfg_component_phasespace_process.txt')

    job = EchoCommands(
        commands = [
            'mkdir -p {name}'.format(name = tbl_dir),
            'rsync -u -t {src} {dest}'.format(
                src = tbl_cfg_process,
                dest = os.path.join(tbl_dir, 'tbl_cfg_component_phasespace_process.txt')
            ),
            '{script} --lumi {lumi} {tbl_dir}'.format(
                script = os.path.join(scripts_dir, 'scripts', 'tbl_n_combine_mc_components.py'),
                lumi = lumi,
                tbl_dir = tbl_dir
            )
        ]
    )
    ret['combine_tables_into_process_{}'.format(name)] = job

    command_format = '{script} --infile-name-prefix tbl_n\.process --outfile-name-prefix tbl_n.process --infile-categories {infile_categories} --categories {categories} --dir {tbl_dir}'
    job = EchoCommands(
        commands = [
            command_format.format(
                script = os.path.join(scripts_dir, 'scripts', 'tbl_n_sum_over_categories.py'),
                infile_categories = 'htbin njetbin mhtbin',
                categories = 'mhtbin',
                tbl_dir = tbl_dir
            ),
            command_format.format(
                script = os.path.join(scripts_dir, 'scripts', 'tbl_n_sum_over_categories.py'),
                infile_categories = 'htbin njetbin mhtbin',
                categories = 'njetbin',
                tbl_dir = tbl_dir
            ),
            command_format.format(
                script = os.path.join(scripts_dir, 'scripts', 'tbl_n_sum_over_categories.py'),
                infile_categories = 'htbin njetbin',
                categories = 'htbin',
                tbl_dir = tbl_dir
            ),
            command_format.format(
                script = os.path.join(scripts_dir, 'scripts', 'tbl_n_sum_over_categories.py'),
                infile_categories = 'htbin njetbin',
                categories = 'njetbin',
                tbl_dir = tbl_dir
            )
        ]
    )
    ret['aggregate_categories_{}'.format(name)] = job

    return ret

##__________________________________________________________________||
if __name__ == '__main__':
    main()
