# This script reads in the output of 03-find_matchup.py. It edits the L2 extensions to be L1a extensions. 
# This script saves out 2 new files.  One file records the field id, granule id, and L2 download url
# The other file is a subset of the first, but only contains rows of unique L2 download urls.

def main():
    
    import argparse
    import pandas as pd
    import re
    
    
    parser = argparse.ArgumentParser(description='''\
      This script changes the L2 satellite granule download\
      urls to the L1A download url. Note that the L2 urls as found from CMR may differ from the earthdata direct data acces L1A urls.  I have found that for modis (both terra and aqua) on CMR the time sting in the urls end with 01, whereas on direct data access they end with 00.  In this script I will specify that correction.  This is not true of seawifs and viirs urls.''')

    parser.add_argument('--L2granlinksFile', nargs=1, type=str, required=True, help='''\
      Full path of input file that contains the CMR L2 granule info. \ The file must be the output of the find_matchup search.''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
      File path for the output file which contains the full list of stations matched to \
      granule names and l1a urls. This will have duplicates of the granule info, but a unique \
      station list.''')

    args=parser.parse_args()
    dict_args=vars(args)
    
    filepath = dict_args['L2granlinksFile'][0]
    ofilepath = dict_args['ofile'][0]
    
    grandf = pd.read_csv(filepath, names=['lat','lon','datetimes','station','granurls','wlon','slat','elon','nlat'])
    
    # For Modis urls, replace 1.L2 from CMR urls with 0.L2 for earthdata direct data access urls:
    grandf.loc[grandf['granurls'].str.contains('MODIS'), 'granurls'] = [gurl.replace('1.L2','0.L2') for gurl in grandf.loc[grandf['granurls'].str.contains('MODIS'), 'granurls']]
    
    grandf['granurls'] = grandf['granurls'].apply(lambda x : urledit(x))
    
    grandf = grandf.loc[~grandf.granurls.str.contains('_GAC')]
    
    grandf['granid'] = grandf['granurls'].apply(lambda x : getgranname(x))
    
    # The full list of stations matched to granule names and urls (contains duplicate granule info)
    outdf = grandf[['station','granid','granurls','wlon','slat','elon','nlat']]
    outdf.to_csv(ofilepath, index=False, header=False)
    
    # Separate the output file by satellite and save files per satellite:
    satellite_names = {'S':'seawifs','A':'aqua','T':'terra','VS':'snpp','V1J':'jpss1','V2J':'jpss2'}
    
    for key in satellite_names:
        sat_df = outdf.loc[outdf['granid'].str[0:-13]==key]
        sat_df.to_csv(ofilepath[0:-4]+'-'+satellite_names[key]+'.csv', index=False, header=False)

    
def urledit(urlstring):
    
    satNames = {'AQUA_MODIS':'A',
            'TERRA_MODIS':'T',
            'SEASTAR_SEAWIFS_MLAC':'S',
            'SNPP_VIIRS':'SNPP_VIIRS',
            'JPSS1_VIIRS':'JPSS1_VIIRS',
            'JPSS2_VIIRS':'JPSS2_VIIRS',
            'NOAA20_VIIRS':'V'}

    extensions = {'AQUA_MODIS':'L1A_LAC.bz2',
            'TERRA_MODIS':'L1A_LAC.bz2',
            'SEASTAR_SEAWIFS_MLAC':'L1A_MLAC.bz2',
            'SNPP_VIIRS':'L1A.nc',
            'JPSS1_VIIRS':'L1A.nc',
            'JPSS2_VIIRS':'L1A.nc',
            'NOAA20_VIIRS':'L1A_JPSS1.nc',
            'NOAA21_VIIRS':'L1A_JPSS2.nc'}
    
    import re
    
    file = re.split('/',urlstring)[-1]
    prefile = re.split('/',urlstring)[:-1]
    path = '/'.join(prefile)+'/'
    
    # Convert to new naming convention
    if (file[1].isalpha())&('VIIRS' not in file):
        
        split = str.split(file, '.')
        satellite = split[0]
        dt = split[1]
        extension = split[2:]
        dt = convertDatetime(dt)

        urlstring = path + satNames[satellite] + dt + '.' + extensions[satellite]
        
    elif (file[1].isalpha())&('VIIRS' in file):
        
        urlstring = urlstring.replace('L2.OC.nc','L1A.nc')
    
    # Maintain Old Naming Convention
    else:
        urlstring = urlstring.replace('L2_','L1A_').replace('_OC','')
        if file[0] in ['A','T','S']:
            urlstring = urlstring.replace('.nc','.bz2')
            
    return urlstring

def convertDatetime(yyyymmddTHHMMSS):
    from datetime import datetime
    
    date,time = str.split(yyyymmddTHHMMSS,'T')
    year = date[0:4]
    month = date[4:6]
    day = date[6:]
    doy = str(datetime(int(year),int(month),int(day)).timetuple().tm_yday)
    
    if len(doy)==1:
        doy = '00' + doy
    elif len(doy)==2:
        doy = '0' + doy
    
    yyyydoyHHMMSS = year + doy + time

    return yyyydoyHHMMSS   
    
def getgranname(urlstring):
    import re
    
    file = re.split('/',urlstring)[-1]
    if 'SNPP_VIIRS' in file:
        split = str.split(file, '.')
        dt = split[1]
        dt = convertDatetime(dt)
        granid = 'VS' + dt
    elif 'JPSS1_VIIRS' in file:
        split = str.split(file, '.')
        dt = split[1]
        dt = convertDatetime(dt)
        granid = 'V1J' + dt
    elif 'JPSS2_VIIRS' in file:
        split = str.split(file, '.')
        dt = split[1]
        dt = convertDatetime(dt)
        granid = 'V2J' + dt
    else:
        granid = re.search('[A-Z][0-9]{13}',urlstring).group()
   
    return granid

if __name__ == "__main__": main()