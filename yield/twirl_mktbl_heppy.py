#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os, sys
import argparse
import logging
import numpy
import pprint

##__________________________________________________________________||
scripts_subdir = os.path.dirname(__file__)
scripts_dir = os.path.dirname(scripts_subdir)
external_dir = os.path.join(scripts_dir, 'external')
sys.path.insert(1, scripts_dir)
sys.path.insert(1, external_dir)
sys.path.insert(1, os.path.join(external_dir, 'alphatwirl'))
sys.path.insert(1, os.path.join(external_dir, 'scribblers'))

##__________________________________________________________________||
import alphatwirl
import fwtwirl

##__________________________________________________________________||
default_heppydir = os.path.join('hdfs', 'SUSY', 'RA1', '80X', 'MC', '20161021_B03', 'ROC_MC_SM')

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('--mc', action = 'store_const', dest = 'datamc', const = 'mc', default = 'mc', help = 'for processing MC')
parser.add_argument('--data', action = 'store_const', dest = 'datamc', const = 'data', help = 'for processing data')
parser.add_argument('-i', '--heppydir', default = default_heppydir, help = 'Heppy results dir')
parser.add_argument('-c', '--components', default = None, nargs = '*', help = 'the list of components')
parser.add_argument('-o', '--outdir', default = os.path.join('tbl', 'out'))
parser.add_argument('-n', '--nevents', default = -1, type = int, help = 'maximum number of events to process for each component')
parser.add_argument('--max-events-per-process', default = -1, type = int, help = 'maximum number of events per process')
parser.add_argument('--force', action = 'store_true', default = False, help = 'recreate all output files')
parser.add_argument('--parallel-mode', default = 'multiprocessing', choices = ['multiprocessing', 'subprocess', 'htcondor'], help = 'mode for concurrency')
parser.add_argument('-p', '--process', default = 4, type = int, help = 'number of processes to run in parallel')
parser.add_argument('-q', '--quiet', default = False, action = 'store_true', help = 'quiet mode')
parser.add_argument('--profile', action = 'store_true', help = 'run profile')
parser.add_argument('--profile-out-path', default = None, help = 'path to write the result of profile')
parser.add_argument('--logging-level', default = 'WARN', choices = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help = 'level for logging')
args = parser.parse_args()

##__________________________________________________________________||
def main():

    configure_logger()

    reader_collector_pairs = configure_reader_collector_pairs()

    run(reader_collector_pairs)

##__________________________________________________________________||
def configure_logger():

    log_level = logging.getLevelName(args.logging_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    names_for_logger = ['alphatwirl', 'fwtwirl']
    for n in names_for_logger:
        logger = logging.getLogger(n)
        logger.setLevel(log_level)
        logger.handlers[:] = [ ]
        logger.addHandler(log_handler)

##__________________________________________________________________||
def configure_reader_collector_pairs():

    ret = [ ]

    ret.extend(configure_scribblers_before_event_selection())

    ret.extend(configure_1st_event_selection())

    ret.extend(configure_tables_after_1st_event_selection())

    path = os.path.join(args.outdir, 'reader_collector_pairs.txt')
    if args.force or not os.path.exists(path):
        alphatwirl.mkdir_p(os.path.dirname(path))
        with open(path, 'w') as f:
            pprint.pprint(ret, stream = f)

    return ret

##__________________________________________________________________||
def configure_scribblers_before_event_selection():

    scribblers = [ ]

    from scribblers.heppy import ComponentName
    from scribblers.essentials import FuncOnNumpyArrays
    scr_ = [
        ComponentName(),
        FuncOnNumpyArrays(src_arrays = ['mht40_pt', 'met_pt'], out_name = 'MhtOverMet', func = numpy.divide),
    ]
    scribblers.extend(scr_)

    #
    ret = [(r, alphatwirl.loop.NullCollector()) for r in scribblers]
    return ret

##__________________________________________________________________||
def configure_1st_event_selection():

    path_cfg = dict(All = (
        'ev : ev.cutflowId[0] == 1 # Signal',
        'ev : ev.nIsoTracksVeto[0] <= 0',
        'ev : ev.nJet40Fwd[0] == 0',
        'ev : ev.nJet40failedId[0] == 0',
        'ev : ev.nJet40[0] >= 2',
        'ev : -2.5 < ev.jet_eta[0] < 2.5',
        'ev : 0.1 <= ev.jet_chHEF[0] < 0.95',
        # 'ev : ev.nJet100[0] >= 1',
        'ev : ev.ht40[0] >= 400',
        'ev : ev.mht40_pt[0] >= 200',
        'ev : ev.MhtOverMet[0] < 1.25',
    ))

    #
    eventSelection = alphatwirl.selection.build_selection(
        path_cfg = path_cfg,
        AllClass = alphatwirl.selection.modules.AllwCount,
        AnyClass = alphatwirl.selection.modules.AnywCount,
        NotClass = alphatwirl.selection.modules.NotwCount
    )

    eventselection_path = os.path.join(args.outdir, 'eventselection.txt')
    if args.force or not os.path.exists(eventselection_path):
        alphatwirl.mkdir_p(os.path.dirname(eventselection_path))
        with open(eventselection_path, 'w') as f:
            pprint.pprint(path_cfg, stream = f)

    tbl_cutflow_path = os.path.join(args.outdir, 'tbl_cutflow.txt')

    resultsCombinationMethod = alphatwirl.collector.ToTupleListWithDatasetColumn(
        summaryColumnNames = ('depth', 'class', 'name', 'pass', 'total')
    )
    deliveryMethod = alphatwirl.collector.WriteListToFile(tbl_cutflow_path)
    collector = alphatwirl.loop.Collector(resultsCombinationMethod, deliveryMethod)

    ret = [(eventSelection, collector)]
    return ret

##__________________________________________________________________||
def configure_tables_after_1st_event_selection():

    Binning = alphatwirl.binning.Binning
    Echo = alphatwirl.binning.Echo
    Round = alphatwirl.binning.Round
    RoundLog = alphatwirl.binning.RoundLog
    echo = Echo(nextFunc = None)
    echoNextPlusOne = Echo()

    njetbin = Binning(boundaries = (0, 2, 3, 6))
    htbin = Binning(boundaries = (0, 400, 800, 1200))
    mhtbin = Binning(boundaries = (0, 200, 400, 600))

    tblcfg = [
        dict(keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'alphaT'),   keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'alphaT'),   binnings = (htbin, njetbin, mhtbin, Round(0.02, 0.5, max = 5, overflow_bin = 5))),
        dict(keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'minbDphi'), keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'minbDphi'), binnings = (htbin, njetbin, mhtbin, Round(0.05, 0, min = 0))),
        dict(keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'minDphi'),  keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'minDphi'),  binnings = (htbin, njetbin, mhtbin, Round(0.05, 0, min = 0))),
        dict(keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'mht40_pt'), keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'mht'),      binnings = (htbin, njetbin, mhtbin, Round(20, 0, max = 1200, overflow_bin = 1200))),
        dict(keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'met_pt'),   keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'met'),      binnings = (htbin, njetbin, mhtbin, Round(20, 0, max = 1200, overflow_bin = 1200))),
        dict(keyAttrNames = ('ht40', 'nJet40', 'mht40_pt', 'jet_pt'),   keyOutColumnNames = ('htbin', 'njetbin', 'mhtbin', 'jet_pt'),   keyIndices = (None, None, None, 0),   binnings = (htbin, njetbin, mhtbin, RoundLog(0.05, 100, min = 100))),
    ]

    tableConfigCompleter = alphatwirl.configure.TableConfigCompleter(
        defaultSummaryClass = alphatwirl.summary.Count,
        defaultOutDir = args.outdir,
        createOutFileName = alphatwirl.configure.TableFileNameComposer(default_prefix = 'tbl_n.component')
    )

    tblcfg = [tableConfigCompleter.complete(c) for c in tblcfg]
    if not args.force:
        tblcfg = [c for c in tblcfg if c['outFile'] and not os.path.exists(c['outFilePath'])]

    ret = [alphatwirl.configure.build_counter_collector_pair(c) for c in tblcfg]
    return ret

##__________________________________________________________________||
def run(reader_collector_pairs):

    htcondor_job_desc_extra_request = ['request_memory = 250']

    # https://lists.cs.wisc.edu/archive/htcondor-users/2014-June/msg00133.shtml
    # hold a job and release to a different machine after a certain minutes
    htcondor_job_desc_extra_resubmit = [
        'expected_runtime_minutes = 10',
        'job_machine_attrs = Machine',
        'job_machine_attrs_history_length = 4',
        'requirements = target.machine =!= MachineAttrMachine1 && target.machine =!= MachineAttrMachine2 &&  target.machine =!= MachineAttrMachine3',
        'periodic_hold = JobStatus == 2 && CurrentTime - EnteredCurrentStatus > 60 * $(expected_runtime_minutes)',
        'periodic_hold_subcode = 1',
        'periodic_release = HoldReasonCode == 3 && HoldReasonSubCode == 1 && JobRunCount < 3',
        'periodic_hold_reason = ifthenelse(JobRunCount<3,"Ran too long, will retry","Ran too long")',
    ]

    # http://www.its.hku.hk/services/research/htc/jobsubmission
    # avoid the machines "smXX.hadoop.cluster"
    # operator '=!=' explained at https://research.cs.wisc.edu/htcondor/manual/v7.8/4_1HTCondor_s_ClassAd.html#ClassAd:evaluation-meta
    htcondor_job_desc_extra_blacklist = [
        'requirements=!stringListMember(substr(Target.Machine, 0, 2), "sm,bs")'
    ]

    ## htcondor_job_desc_extra = htcondor_job_desc_extra_request + htcondor_job_desc_extra_resubmit
    htcondor_job_desc_extra = htcondor_job_desc_extra_request + htcondor_job_desc_extra_blacklist

    fw = fwtwirl.FrameworkHeppy(
        outdir = args.outdir,
        heppydir = args.heppydir,
        datamc = args.datamc,
        force = args.force,
        quiet = args.quiet,
        parallel_mode = args.parallel_mode,
        htcondor_job_desc_extra = htcondor_job_desc_extra,
        process = args.process,
        user_modules = ('scribblers', ),
        max_events_per_dataset = args.nevents,
        max_events_per_process = args.max_events_per_process,
        profile = args.profile,
        profile_out_path = args.profile_out_path

    )
    fw.run(
        components = args.components if args.components else None,
        # give None to components if args.components is an empty
        # list. fw.run() runs all components if components is None
        # and runs no components if components is an empty list.
        reader_collector_pairs = reader_collector_pairs
    )

##__________________________________________________________________||
if __name__ == '__main__':
    main()
