#!/bin/bash

#PBS -N troubleshoot-gs
#PBS -q route

#PBS -l ncpus=4,mem=4gb
#PBS -l walltime=4:00:00
#PBS -l vnode=c4-4
#PBS -o /mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/logs
#PBS -e /mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/logs

# Load modules and environment
module use /mod/bigelow
module load anaconda3
source activate ~/ocssw_env

#/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev/01b-main-gnatsat.sh
/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev/z01c-troubleshoot.sh
