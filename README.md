# concur-anon-comp
An example analysis code with alphatwirl and data frames

## quick instructions

### check out

Log in to soolin.

Move to a work dir:
```bash
cd /some/work/dir/
```

The code runs in any envriomment with Python 2.7, ROOT 6, and several other common libraries.

In this example, we will run in `cmsenv`. 

Source `cmsset_default.sh` if it is not done yet.
```bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
```

Check out `cmsenv` if it is not done yet.
```bash
export SCRAM_ARCH=slc6_amd64_gcc530
cmsrel CMSSW_9_2_5_patch1
```

Enter the `cmsenv`:
```bash
cd CMSSW_9_2_5_patch1/src/
cmsenv
cd ../../
```

Check out this repo:
```bash
git clone --recursive https://github.com/TaiSakuma/concur-anon-comp.git
```

### run

Print the commants to be executed
```bash
./concur-anon-comp/yield/print_commands_heppy.py
```

If you want to execute them all, you can just pipe to sh.

The option `--print-jobs` shows the list of the jobs.
```bash
./concur-anon-comp/yield/print_commands_heppy.py --print-jobs
```
It will print:
```bash
summarize_trees_SM
combine_tables_into_process_SM
aggregate_categories_SM
```

(Optinally) summarize trees and create data frames with alphatwirl
```bash
./concur-anon-comp/yield/print_commands_heppy.py summarize_trees_SM | sh -x  # (optional)
```

The output of the above optional cmmand is stored in this repo. So if you skip the above command, you can copy the result
```bash
cp -r concur-anon-comp/example_tbl/tbl_20170710_01 . # (alternative to the above command)
```

You should look at the files in the dir `tbl_20170710_01/SM/`.

Combine components into processes:
```bash
./concur-anon-comp/yield/print_commands_heppy.py combine_tables_into_process_SM | sh -x
```

You should look at the files in the dir `ls tbl_20170710_01/SM/` again and understand what happened.

Aggregate categoreis:
```bash
./concur-anon-comp/yield/print_commands_heppy.py aggregate_categories_SM | sh -x
```
Again look at `tbl_20170710_01/SM/` and understand what happened.


