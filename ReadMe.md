### Matchup Workflow Setup:

* include data directory structure
* ocssw env .yml file
* update luts beforehand
* error look-up-table


### Processing:

* make sure ncpus agree in the three locations: submission, scripts 05 and 06


### Inputs:

### Outputs:

### Scripts:

**01-mergeUwDiscrete.py:** This is a GNATS specific script.  It takes in a database extracted underway file, and a database extracted discrete file.  It merges the discrete and underway data based on nearest time within a 5 minute time tolerance. It outputs a merged field dataframe.

Additionally, this script addresses some formatting issues that needed to be corrected before the merge. These issues still should be corrected within the GNATS database itself. The issues corrected include: inconsistent whitespace in the underway cruise column, bbprime std dev not calculated for many years, Lt and Li radiometry columns were swapped for a handful of cruises, half of the discrete dataset depths were set to zero and half to null, discrete column HPLC was incorrectly labeled as HLPC, some of the cruise names did not agree with the corresponding datetimes, there were a handful of duplicated datetime entries.

**02-datasetFormatting.py:** This script is set up to take as input either an underway file, a discrete file, or a merged file. It has only been tested on the merged file. This script also requires as input an error lut that defines the error for each column of the input dataframe. If an error lut is not input, the script will break but output a table that needs to be filled in with errors. This script performs a number of formatting checks, and identifies gross outliers due to typos:
* drops rows with nan values in the latitude and longitude columns (essenial for find_matchup.py to work)
* checks for consistency with string formatting
* checks for consistency with the type of cell values within a column
* checks for consistency in positive and negative signs
* checks for consistency in order of magnitude of entries in column
** if any of these checks do not check out, then the value is nullified, and the value, its dataframe location, and the check that tripped is recorded in an output error record file.

After gross outliers are nullified, error columns utilizing the error LUT are calculated and saved within the dataframe.

**NOTE:** This script calculates and saves as a new column UW PIC from bbprime and bbstar.  The script uses bbstar = 1.628.  However, this bbstar will change for future satellite processing. **BBstar will need to be updated.**

* This script also calculates RRS columns

The script outputs a formatted field dataframe.

**03-createSeabassStationList.py:** This script takes in a formatted field dataframe.  It records the datetime, ID, longitude, and latitude in a seabass formatted file. The seabass format is necessary for the next script.

**04-find_granules.sh:** This script takes the seabass file as input. It feeds each row (each field datapoint) from the seabass file into 04a-find_matchup.py. Once this is complete, it takes the granule links output from find_matchup.py and sends the links to 04b-editurls.py.

**04a-find_matchup.py:** This script takes in the seabass file and searches the EarthData CMR for satellite granules. We have this script set to search for viirs, seawifs, modis aqua, and modis terra granules. The maximum time difference is set to 6 hours.
Note that at the end of 2017, the VIIRS platform name in CMR changed from NPP to Suomi-NPP. In order to download all relevant viirs files across this name change, we need to search for both NPP and Suomi-NPP.
Note that CMR only contains L2 files. We use this script to find the L2 file granule link name and record that name. Ultimately we want to download L1a files though. See next script.

**04b-editurls.py:** This script edits the L2 file names (specifically the extensions) recorded from find_matchup.py, and edits them to have L1a file extensions.

**05-satproc_initialize.sh:** This script reads in a text file with seawifs, modis, and viirs download urls.  It calls on one of three satellite-specific processing scripts that download the satellite granules via wget into a specific satellite directory structure. A par file is required that specifies which products to download, at which resolution.  This script is also set up to download sst if specified. Thus the script requires two par files: one including sst, one excluding sst. Furthermore, the script requires a cookies file containing the user's earth data login credentials. See _____________ for more information on setting up this cookie file. This script also forks the workflow so that multiple satellite files can be downloaded at once. 

**05a-modis-workflow.sh:**
**05b-seawifs-workflow.sh:**  
**05c-viirs-workflow.sh:** 
These three scripts take in an L1a download url, use wget to download the L1a file, process the L1a file to L1b, and the L1b to L2.  Output is sent to an output log rather than written interactively in the terminal.

**06-matchup_readGranLinks.sh:** This script reads in the full-granules file output from script 04, which contains the field ID, the granule ID, and the granule download url.  For each field to satellite matchup, it feeds the field id and granule id into 06a-matchup_outputDatarows.py. It forks the process so that 06a-matchup_outputDatarows.py processes multiple matchups at once.

**06a-matchup_outputDatarows.py:** This script takes in a field id and its matching granule id, as well as it reads in the field dataframe. It selects out the field data from the field dataframe associated with the field id.  It imports the satellite file from the granule id.  It performs a great circle distance check. Up until now, the field id and granule id are matched by a 6 hour time window. In this script, if the great circle distance between the field data point and the nearest satellite pixel in the granule is less than 1 km, the processing continues. Otherwise, the matchup is recorded in an excluded matchup log, and processing moves on to the next matchup.  For matchups within 1 km, this script selects a 5x5 pixel grid around the nearest matchup pixel. It calculates statistics (mean, median, stdev, etc) from the pixel grid for each satellite product.  It merges the field data with the pixel grid statistics, and outputs this 'matchup datarow' into it's own csv saved into a matchup directory.

**07-mergeDatarows.py:** This script reads in all matchups single row csvs saved in the matchup directory and assembles a unified matchup dataframe.  This script uses pd.concat to assemble the rows together. Concatenation is a slow and heavy memory usage process.  This script takes upwards of 12 hours to run.

**08-groupWaveBands.py:** This script reads in the matchup dataframe, which contains matchups for seawifs, viirs, and modis.  Seawifs, Viirs, and Modis record data at slightly different wavelengths. This script groups the wavelengths (for each product) into 10 color bands by taking the mean of the data that is grouped and saves out a new dataframe with color bands included.

**09-gnatsatSpecificFormatting.py:** This script reads in the matchup dataframe with color bands.  It averages multiple field data points that map to a single satellite pixel.  It also organizes the columns, with identification columns (Field id, granule id, cruise, station, datetime, and gps information) on the left, then field columns, then satellite columns on the right.

