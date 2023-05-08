#!/bin/bash

#PBS -N publish-matchups
#PBS -q route

#PBS -l ncpus=40,mem=512gb
#PBS -l walltime=24:00:00
#PBS -l vnode=c4-2
#PBS -o /mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/logs
#PBS -e /mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/logs

# Load modules and environment
module use /mod/bigelow
module load anaconda3
source activate ~/ocssw_env

/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev/x02-main.sh