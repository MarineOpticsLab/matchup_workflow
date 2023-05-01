# This script reads in the output of 03-find_matchup.py. It edits the L2 extensions to be L1a extensions. 
# This script saves out 2 new files.  One file records the field id, granule id, and L2 download url
# The other file is a subset of the first, but only contains rows of unique L2 download urls.

def main():
    
    import argparse
    import pandas as pd
    import re
    
    
    parser = argparse.ArgumentParser(description='''\
      This script changes the L2 satellite granule download\
      urls to the L1A download url.''')

    parser.add_argument('--L2granlinksFile', nargs=1, type=str, required=True, help='''\
      full path of input file that contains the granule info. \ The file must be the output of the find_matchup search i.e. a \
granule-links.csv file''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
      File path for the output file which contains the full list of stations matched to \
      granule names and l1a urls. This will have duplicates of the granule info, but a unique \
      station list.''')

    args=parser.parse_args()
    dict_args=vars(args)
    
    filepath = dict_args['L2granlinksFile'][0]
    
    grandf = pd.read_csv(filepath, names=['lat','lon','datetimes','station','granurls','wlon','slat','elon','nlat'])
    
    grandf['granurls'] = grandf['granurls'].apply(lambda x : urledit(x))
    
    grandf = grandf.loc[~grandf.granurls.str.contains('_GAC')]
    
    grandf['granid'] = grandf['granurls'].apply(lambda x : getgranname(x))
    
    # The full list of stations matched to granule names and urls (contains duplicate granule info)
    outdf1 = grandf[['station','granid','granurls','wlon','slat','elon','nlat']]
    outdf1.to_csv(dict_args['ofile'][0], index=False, header=False)

    
def urledit(urlstring):
    
    satNames = {'AQUA_MODIS':'A',
            'TERRA_MODIS':'T',
            'SEASTAR_SEAWIFS_MLAC':'S',
            'SNPP_VIIRS':'V',
            'NOAA20_VIIRS':'V'}

    extensions = {'AQUA_MODIS':'L1A_LAC.bz2',
            'TERRA_MODIS':'L1A_LAC.bz2',
            'SEASTAR_SEAWIFS_MLAC':'L1A_MLAC.bz2',
            'SNPP_VIIRS':'L1A_SNPP.nc',
            'NOAA20_VIIRS':'L1A_JPSS1.nc',
            'NOAA21_VIIRS':'L1A_JPSS2.nc'}
    
    import re
    
    file = re.split('/',urlstring)[-1]
    prefile = re.split('/',urlstring)[:-1]
    path = '/'.join(prefile)+'/'
    
    # Convert to new naming convention
    if file[1].isalpha():
        
        split = str.split(file, '.')
        satellite = split[0]
        dt = split[1]
        extension = split[2:]
        dt = convertDatetime(dt)

        urlstring = path + satNames[satellite] + dt + '.' + extensions[satellite]
    
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
    
    return re.search('[A-Z][0-9]{13}',urlstring).group()

if __name__ == "__main__": main()