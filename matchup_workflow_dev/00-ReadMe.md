# Matchup Workflow:

### Overview:

This repository takes in a field dataframe with valid Id, Latitude, Longitude, and Datetime Fields. Using this information, the scripts search the EarthData Common Metadata Repository (CMR) for satellite files that match up to the field data within a 6 hour window. The matchup L2 granule urls are written into a text file. The L2 extensions are edited to L1a extensions. Through wget, regions specified via bounding boxes are extracted and downloaded from the L1a files.   A 5x5 pixel grid is drawn around the pixel nearest in location to the field data point. Pixel grid statistics as per Bailey and Werdell are reported.  The satellite data is then merged to the field data, ultimately outputting a matched up field-satellite-dataframe.




### Processing Notes:

Before running these scripts, the ocssw satellite look up tables should be updated. 
* Activate the ocssw environment.
* Enter command in terminal: update_luts all

PBS Scheduler:

These scripts were set up to be run via linux submission using  PBS job scheduler.  Note that some of the scripts (06 (satellite processing) and 09 (matching satellite data row by row with field data)) are resource intensive and are therefore, the jobs are forked to speed up processing. If user is not using a PBS scheduler, user should comment out the following three lines wherever found in the main script:

*   while [ $(jobs | wc -l) -ge $ncpus ] ; do
*      sleep 1s
*   done

### Scripts:

#### 02-seabass-station-list.py:
**Description:** This script takes in a field data file which must contain an ID field with a unique id/station for each record.  It outputs a SeaBASS formatted data file, complete with header, including datetime, station, longitude, and latitude for each record.

**Input Files:** Field data file which must contain a unique id for each record.

**Output Files:** SeaBASS station list containing field datetime and location info. 

#### 03-find-matchup.py:
**Description:** This script performs searches of the CMR for satellite granule names and download links. Originally written by J.Scott on 2016/12/12, then modified by Inia Soto, Catherine Mitchell, and Sunny Pinkham.  The original script has been heavily modified to suit current purposes and procedures, including updates to include satellites launched after the original script was written. Returns granules names for granules containing field data location, which defaults to within a +-3 hour (6 hour total) time window.

**Input Files:** SeaBASS station list containing field data datetime and location info.

**Output Files:** L2-granule-links file containing field id, location, and datetime matched up to CMR L2 urls and bounding box regions specified by field coordinates +- 1 degree of longitude/latitude. 

#### 04-edit-L2-urls.py:
**Description:** This scripts edits the L2 urls as found on CMR to formatting consistent with corresponding L1a urls found on earthdata direct data access. Note that the datetime strings within the urls need to be re-formatted between CMR and direct data access. OB.DAAC file naming convention was updated in 2022. This script reflects those updates. If file naming conventions are changed in future, this script will need to be updated.

**Input Files:** L2-granule-links file output from 03-find-matchup.py containing CMR L2 granule urls.

**Output Files:** 
* L1a-granule-links file containing L1a granule links of matched up satellite files as found on Direct Data Access. This file contains data for all requested satellites.
* L1a-granule-links satellite-specific file for each specified satellite containing only urls for matchups with specified satellite.

#### 05-create-L1a-download-list.py:
**Description:** In this script, for all field data records that map to the same L1a granule, we expand bounding box for each record to the maximum boundaries (so that the region extracted will contain all relevant field data matchups).  Additionally, we then only write the unique L1a granules with expanded bounding boxes to the output file.

**Input Files:** L1a-granule-links file containing records for all field data matchuped up to earthdata direct data access L1a granule urls.

**Output Files:**
* L1a-download-urls: a unique list of L1a granules to download for all specified satellites.
* L1a-download-urls satellite specific file for each specified satellite in workflow.

#### 06*-satellite-workflow.sh:
**Description:** Satellite-specific scripts that process L1a files to L2. This script creates a directory tree: satellite/year/doy/granid.L2. To use these scripts, the satellite specific list of unique L1a urls are read in line by line (via shell script). The granule link and bounding box are fed into the 06-satellite-workflow-script.

**Note:** This repository was built on seawifs, aqua, terra, snpp, jpss1, and jpss2. This repository contains 06-satellite-workflow.sh scripts for each of these six satellites. If user specifies another satellite, user must develop shell script workflow for specified satellite.

**Requirements:** This script requires user to have a netrc file containing earthdata log in credentials in user's home directory.

**Input Files:** Satellite specific unique list of earthdata direct data access formatted L1a urls.

**Output Files:** Downloaded L2 files.

#### 07-report-L2-percent-processed.py:
**Description:** Prints out the total percentage of granule links that successfully processed to L2. Also reports percentage per satellite.

#### 08-partition-field-by-satellite.py:
**Description:** For each satellite, this script subsets and saves out the field data that matches up to the satellite.  This makes the next step--merging the field data with the satellite data record by record--much faster.

**Input Files:** Field data file.

**Output Files:** Satellite-specific field-matchup data files.

#### 09-matchup-datarows.py:
**Description:** This script opens up the L2 files, calculates pixel grid statistics as described in Bailey and Werdell, merges the satellite data record by record to the field data, and outputs an individual csv of a single row for each and every matchup record. It also checks that the satellite file/pixel is within 1km of the field data point. If not, the merge does not happen.

**Note:** This statistical processing and merge is a lengthy, resource heavy process. The processing is far more efficient if broken up per satellite. Therefore, satellite specific granule links files containing matched up field ids (L1a-granlinks files), are fed separately to this script.

**Input Files:** 
* satellite-specific-field-datafile
* satellite-specific granule links file containing matched up field ids

**Output Files:**
* matchup datarows: single row csvs containing field data matched to satellite data. 
* satellite-specific excluded matchup log text file.

#### 10-merge-datarows.py:
**Description:** This script reads in individual matchup datarows within a given directory and merges them into a single dataframe.

**Note:** The merge function is resource heavy. The processing is more efficient to merge in smaller chunks, per satellite, then to merge the per-satellite matchup dataframes together.

**Input Files:**
* satellite-specific matchup datarows

**Output Files:**
* satellite-specific matchup dataframe

#### 11-merge-matchup-dfs.py:
**Description:** This script reads in the satellite specific matchup dataframes and concats them into a single matchup dataframe. Note that this script requires two matchup dataframes and by default takes in up to 6 matchup dataframes. If user has more tha 6 satellite specific matchup dataframes to merge together, user must use parser to add more arguments to this script.

**Input Files:**
* satellite-specific matchup dataframes

**Output Files:**
* single matchup dataframe containing data from all satellites.





**INCLUDE NETRC FILE/COOKIES FILE**