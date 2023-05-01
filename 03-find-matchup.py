#!/usr/bin/env python3
# coding: utf-8

"""
A script to perform searches of the EarthData Common Metadata Repository (CMR)
for satellite granule names and download links.
written by J.Scott on 2016/12/12 (joel.scott@nasa.gov)
Modified by Inia Soto on 2020/08/17 (inia.m.sotoramos@nasa.gov)
Replaced wget function with python requests
Modified by Cath Mitchell on 2022/01/20 (cmitchell@bigelow.org)
1) added an input argument to save the found granule links to
2) made two new functions `processandtrack_CMRreq` and `printtofile_CMRreq` 
which are slightly adapted versions of `process_CMRreq` and `print_CMRreq`
to connect granules to each row of the input sb file
3) modified 3 lines of code to call those new functions and include the
station in the for loop over the sb file. MUST INCLUDE A COLUMN IN THE 
sb FILE THAT IS CALLED station
Note that this edit likely will cause problems for the case where a sb file
AND lats and lons are provided at the command line as inputs 
(e.g. with `--elat`, etc), but I don't imagine that I will ever want to 
do that, and if I do, I'll just use the NASA version of the script, rather
than my modified one. I just wanted to make the minimal amount of edits to 
the `find_matchup.py` file, rather than slim it down to only the essentials 
for my usage, so if there are updates to the file by NASA in the future, 
then it's not as big a rewrite for me.
Modified by Sunny Pinkham on 2022/07/01:
1) Modified dict_plat structure.  dict_plat['platform'] now contains a list, 
rather than a single value (even if it is a list of only 1 element long).  
This was necessary for the VIIRS workflow. Prior to 2018, in CMR, the VIIRS
platform is tagged as 'NPP'. Post January 2018, VIIRS platform is tagged as 
'Suomi-NPP'.  It is necessary to include both platforms in order to attain 
Viirs matchups across 2017/2018. 
"""

def main():

    import argparse
    import os
    from datetime import timedelta
    from math import isnan
    from collections import OrderedDict

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description='''\
      This program perform searches of the EarthData Search (https://search.earthdata.nasa.gov/search) Common Metadata
      Repository (CMR) for satellite granule names given an OB.DAAC satellite/instrument and lat/lon/time point or range.
      
      Outputs:
         1) a list of OB.DAAC L2 satellite file granule names that contain the input criteria, per the CMR's records.
         2) a list of public download links to fetch the matching satellite file granules, per the CMR's records.
      
      Inputs:
        The argument-list is a set of --keyword value pairs.
      Example usage calls:
         fd_matchup.py --sat=modist --slat=23.0 --slon=170.0 --stime=2015-11-16T09:00:00Z --max_time_diff=8
         fd_matchup.py --sat=modist --stime=2015-11-15T09:00:00Z --etime=2015-11-17T09:00:00Z --slat=23.0 --elat=25.0 --slon=170.0 --elon=175.0
         fd_matchup.py --sat=modist --max_time_diff=4 --seabass_file=[your SB file name].sb
         fd_matchup.py --sat=modist --slat=23.0 --slon=170.0 --stime=2015-11-16T09:00:00Z --max_time_diff=8 --get_data=[Your path]
      Caveats:
        * This script is designed to work with files that have been properly
          formatted according to SeaBASS guidelines (i.e. Files that passed FCHECK).
          Some error checking is performed, but improperly formatted input files
          could cause this script to error or behave unexpectedly. Files
          downloaded from the SeaBASS database should already be properly formatted, 
          however, please email seabass@seabass.gsfc.nasa.gov and/or the contact listed
          in the metadata header if you identify problems with specific files.
        * It is always HIGHLY recommended that you check for and read any metadata
          header comments and/or documentation accompanying data files. Information 
          from those sources could impact your analysis.
        * Compatibility: This script was developed for Python 3.5.
        * Requires a valid .netrc file in the user home ($HOME), e.g.:
          machine urs.earthdata.nasa.gov login USERNAME password PASSWD
      ''',add_help=True)

    parser.add_argument('--sat', nargs=1, required=True, type=str, choices=['modisa','modist','viirsn','viirsj1','goci','meris','czcs','octs','seawifs'], help='''\
      String specifier for satellite platform/instrument
      
      Valid options are:
      -----------------
      modisa  = MODIS on AQUA
      modist  = MODIS on TERRA
      viirsn  = VIIRS on Suomi-NPP
      viirsj1 = VIIRS on JPSS-1/NOAA-20
      meris   = MERIS on ENVISAT
      goci    = GOCI on COMS
      czcs    = CZCS on Nimbus-7
      seawifs = SeaWiFS on OrbView-2
      octs    = OCTS on ADEOS-I
      ''')

    parser.add_argument('--data_type', nargs=1, type=str, default=(['*']), choices=['oc','iop','sst'], help='''\
      OPTIONAL: String specifier for satellite data type
      Default behavior returns all product suites
      
      Valid options are:
      -----------------
      oc   = Returns OC (ocean color) product suite
      iop  = Returns IOP (inherent optical properties) product suite
      sst  = Returns SST product suite (including SST4 where applicable)
      ''')

    parser.add_argument('--max_time_diff', nargs=1, type=float, default=([3]), help=('''\
      Maximum time difference between satellite and in situ point
      OPTIONAL: default value +/-3 hours
      Valid values: decimal number of hours (0-36)
      Use with --seabass_file OR --stime
      '''))

    parser.add_argument('--seabass_file', nargs='+', type=argparse.FileType('r'), help='''\
      Valid SeaBASS file name or list of file names
      File must contain latitude, longitude, and date-time information as fields.
      ''')
    
    parser.add_argument('--includeGnatsCheck', default=0, nargs=1, type=int, required=True, help=('''\
      Input either 0 or 1. Default is zero.  If set to 1, include a check for Gnats data in order to specify a Gulf or Maine bounding box. If ID starts with 's', is considered GNATS as defined by function isGnats.
      '''))

    parser.add_argument('--verbose', default=False, action='store_true', help=('''\
      OPTIONAL: Displays HTTP requests for each Earthdata CMR query.
      '''))
    
    parser.add_argument('--output_file', nargs=1, type=str, help='''\
      A file name to save granule links to.
      ''')

    
    args=parser.parse_args()
    
    # Create dictionary of lists of CMR platform, instrument, collection names
    dict_plat = {}
    dict_plat['modisa']  = ['MODIS',['AQUA'],'MODISA_L2_']
    dict_plat['modist']  = ['MODIS',['TERRA'],'MODIST_L2_']
    dict_plat['viirsn']  = ['VIIRS',['Suomi-NPP','NPP'],'VIIRSN_L2_']
    dict_plat['viirsj1'] = ['VIIRS',['NOAA-20'],'VIIRSJ1_L2_']
    dict_plat['meris']   = ['MERIS',['ENVISAT'],'MERIS_L2_']
    dict_plat['goci']    = ['GOCI',['COMS'],'GOCI_L2_']
    dict_plat['czcs']    = ['CZCS',['Nimbus-7'],'CZCS_L2_']
    dict_plat['seawifs'] = ['SeaWiFS',['OrbView-2'],'SeaWiFS_L2_']
    dict_plat['octs']    = ['OCTS',['ADEOS-I'],'OCTS_L2_']

    ### CHECK INPUT ARGUMENTS: ###################################################################
    
    if not args.sat:
        parser.error("you must specify a satellite string to conduct a search")
    else:
        dict_args=vars(args)
        sat = dict_args['sat'][0]

    if sat not in dict_plat:
        parser.error('you provided an invalid satellite string specifier. Use -h flag to see a list of valid options for --sat')

    if dict_args['max_time_diff'][0] < 0 or dict_args['max_time_diff'][0] > 36:
        parser.error('invalid --max_time_diff value provided. Please specify a value between 0 and 36 hours. Received --max_time_diff = ' + str(dict_args['max_time_diff'][0]))
    else:
        twin_Hmin = -1 * int(dict_args['max_time_diff'][0])
        twin_Mmin = -60 * (dict_args['max_time_diff'][0] - int(dict_args['max_time_diff'][0]))
        twin_Hmax = 1 * int(dict_args['max_time_diff'][0])
        twin_Mmax = 60 * (dict_args['max_time_diff'][0] - int(dict_args['max_time_diff'][0]));
        
    ################################################################################################
    ### SEARCH CMR FOR L2 DOWNLOAD URLS ###
    ################################################################################################
        
    for filein_sb in dict_args['seabass_file']:

        ds = check_SBfile(parser, filein_sb.name)           
        granlinks = OrderedDict()
        rowinfo = OrderedDict()
        hits = 0

        ### Set bounding box for downloading L2 files. ######################################################################
        # Define bounding box as +- 1 degree latitude and longitude from the field coordinates in the SeaBASS file.
        for lat,lon,dt,station in zip(ds.lat,ds.lon,ds.datetime,ds.data['station']):
            
            #If is Gnats station, set gnats bounding box
            if (dict_args['includeGnatsCheck'][0]==1)&(isGnats(station)): 
                wlon = -71
                slat = 42
                elon = -66
                nlat = 45
                
            else:
            
                if (lon>=-180)&(lon<-179):
                    wlon = -180
                else:
                    wlon = lon - 1

                if (lon>179)&(lon<=180):
                    elon = 180
                else:
                    elon = lon + 1

                if (lat>=-90)&(lat<-89):
                    slat = -90
                else:
                    slat = lat - 1

                if (lat>89)&(lat<=90):
                    nlat = 90
                else:
                    nlat = lat + 1

            ### Specify time limits for search: ###################################################################
            tim_min = dt + timedelta(hours=twin_Hmin,minutes=twin_Mmin) #use as: tim_min.strftime('%Y-%m-%dT%H:%M:%SZ')
            tim_max = dt + timedelta(hours=twin_Hmax,minutes=twin_Mmax) #use as: tim_max.strftime('%Y-%m-%dT%H:%M:%SZ')

            # For the input satellite, construct a search url based on lat, lon, and time parameters:
            platform = ''
            for entry in dict_plat[sat][1]:
                platform += '&platform=' + entry

            url = 'https://cmr.earthdata.nasa.gov/search/granules.json?page_size=2000' + \
                            '&provider=OB_DAAC' + \
                            '&point=' + str(lon) + ',' + str(lat) + \
                            '&instrument=' + dict_plat[sat][0] + \
                            platform + \
                            '&short_name=' + dict_plat[sat][2] + dict_args['data_type'][0] + \
                            '&options[short_name][pattern]=true' + \
                            '&temporal=' + tim_min.strftime('%Y-%m-%dT%H:%M:%SZ') + ',' + tim_max.strftime('%Y-%m-%dT%H:%M:%SZ') + \
                            '&sort_key=short_name'

            if dict_args['verbose']:
                print(url)

            # The following function sends the url to CMR and returns json formatted search query.
            content = send_CMRreq(url)

            # The following function submits the json query and outputs granule links to matched up satellite files.
            # Also returns corresponding SeaBASS file row/station info, so when batch downloading, we can keep track of which field station corresponds to which satellite file.
            [hits, granlinks, rowinfo] = processandtrack_CMRreq(content, hits, granlinks, rowinfo, lat, lon, dt, station, wlon, slat, elon, nlat)

        # The following function writes the results of the CMR search to a text file. This becomes our output granule links file.
        printtofile_CMRreq(hits, granlinks, dict_plat[sat], args, dict_args, rowinfo)

    return


def check_SBfile(parser, file_sb):
    """ function to verify SB file exists, is valid, and has correct fields; returns data structure """
    #from seabass.SB_support import readSB
    import os
    from SB_support import readSB

    ### Check if sbfile exists: ###############
    if os.path.isfile(file_sb):
        ds = readSB(filename=file_sb, 
                    mask_missing=True, 
                    mask_above_detection_limit=True, 
                    mask_below_detection_limit=True, 
                    no_warn=True)
    else:
        parser.error('ERROR: invalid --seabass_file specified. Does: ' + file_sb + ' exist?')

    ### Check if datetime exists and has correct format: ##################
    ds.datetime = ds.fd_datetime()
    if not ds.datetime:
        parser.error('missing fields in SeaBASS file. File must contain date/time, date/hour/minute/second, year/month/day/time, OR year/month/day/hour/minute/second')
     
    ### Check if lat, lon exist and are not nan:
    ds.lon = []
    ds.lat = []
    if 'lat' in ds.data and 'lon' in ds.data:
        for lat,lon in zip(ds.data['lat'],ds.data['lon']):
            check_lat(parser, float(lat))
            check_lon(parser, float(lon))
            ds.lat.append(float(lat))
            ds.lon.append(float(lon))
    else:
        parser.error('missing headers/fields in SeaBASS file. File must contain lat,lon information')
    
    return ds


def check_lat(parser, lat):
    """ function to verify lat range """
    if abs(lat) > 90.0:
        parser.error('invalid latitude: all LAT values MUST be between -90/90N deg. Received: ' + str(lat))
    return


def check_lon(parser, lon):
    """ function to verify lon range """
    if abs(lon) > 180.0:
        parser.error('invalid longitude: all LON values MUST be between -180/180E deg. Received: ' + str(lon))
    return


def send_CMRreq(url):
    """ function to submit a given URL request to the CMR; return JSON output """
    import requests

    req = requests.get(url)
    content = req.json()

    return content


def processandtrack_CMRreq(content, hits, granlinks, rowinfo, lat, lon, dt, station, wlon, slat, elon, nlat):
    """ function to process the return from a single CMR JSON return
    while keeping track of sb file """
    from collections import OrderedDict
    try:
        hits = hits + len(content['feed']['entry'])
        granlinks[station] = OrderedDict()
        rowinfo[station] = OrderedDict()
        for entry in content['feed']['entry']:
            granid = entry['producer_granule_id']
            granlinks[station][granid] = entry['links'][0]['href']
            rowinfo[station][granid] = [lat, lon, dt, station, wlon, slat, elon, nlat]
    except:
        print('WARNING: No matching granules found for a row. Continuing to search for granules from the rest of the input file...')

    return hits, granlinks, rowinfo


def download_file(url, out_dir):
    '''
    download_file downloads a file
    given URL and out_dir strings
    syntax fname_local = download_file(url, out_dir)
    '''
    import os,subprocess
    import requests
    from pathlib import Path

    out_path = Path(out_dir).expanduser() #even if already a Path object, this convert should work
    path_local = out_path / url.split('/')[-1]

    print('Downloading',url.split('/')[-1],'to',out_path)

    if not out_path.exists():
        os.makedirs(str(out_dir))

    if path_local.exists():

        print(f'{path_local.name} found - deleting and attempting to re-download...')
        path_local.unlink()

    else:

        try:
            print(f'downloading {path_local.name}')
            req2 = requests.get(url)
            with open(path_local.as_posix(), 'wb') as f:
                f.write(req2.content)

        except Exception as e:
            if 'HTTPError' in e:
                print('Error in download_file:',e)
                return -1

            else:
                print('Unknown error:', e)

        if not path_local.exists():
            print('Error in download_file: local copy of file not found; download unsuccessful')
            return -2

        print('Successfully downloaded',url.split('/')[-1],'to',out_path)

    return str(path_local)


def printtofile_CMRreq(hits, granlinks, plat_ls, args, dict_args, rowinfo):
    """" function to print the CMR results from a SB file to a text file """

    if hits > 0:
        unique_hits = 0
        for station in granlinks:
            for granid in granlinks[station]:
                unique_hits = unique_hits + 1

                if '_GAC' in granlinks[station][granid]:
                    continue
                else:
                    with(open(dict_args['output_file'][0],'a')) as file: 
                        file.write(str(rowinfo[station][granid][0])+',')
                        file.write(str(rowinfo[station][granid][1])+',')
                        file.write(str(rowinfo[station][granid][2])+',')
                        file.write(str(rowinfo[station][granid][3])+',')
                        file.write(granlinks[station][granid]+',')
                        file.write(str(rowinfo[station][granid][4])+',')
                        file.write(str(rowinfo[station][granid][5])+',')
                        file.write(str(rowinfo[station][granid][6])+',')
                        file.write(str(rowinfo[station][granid][7])+'\n')

        print('Number of granules found: ' + str(unique_hits))
        
    else:
        print('WARNING: No granules found for ' + plat_ls[1] + '/' + plat_ls[0] + ' and any lat/lon/time inputs.')

    return

def isGnats(matchup_id):
    gnatsStatus = matchup_id[0] == 's'
    return gnatsStatus


if __name__ == "__main__": main()