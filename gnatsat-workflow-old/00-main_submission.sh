#!/bin/bash

#PBS -N satelliteProcessing  
#PBS -q route

#PBS -l ncpus=40,mem=128gb
#PBS -l walltime=96:00:00
#PBS -o /mnt/storage/labs/mitchell/projects/matchup-workflow-data/logs
#PBS -e /mnt/storage/labs/mitchell/projects/matchup-workflow-data/logs

# Load modules and environment
module use /mod/bigelow
module load anaconda3
source activate ~/ocssw_env

/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup-workflow/00a-main.sh -u uw-file.txt -d discrete-file.txt -s /mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup-workflow -D /mnt/storage/labs/mitchell/projects/matchup-workflow-data -t 1 -c /home/spinkham/.urs_cookies -g 1 -n 40