# This script reads in the output of 04a-find_matchup.py. It edits the L2 extensions to be L1a extensions. 
# This script saves out 2 new files.  One file records the field id, granule id, and L2 download url
# The other file is a subset of the first, but only contains rows of unique L2 download urls.

def main():
    
    import argparse
    import pandas as pd
    import re
    
    
    parser = argparse.ArgumentParser(description='''\
      This script changes the L2 satellite granule download\
      urls to the L1A download url''')

    parser.add_argument('--ifile', nargs=1, type=str, required=True, help='''\
      full path of input file that contains the granule info. \ The file must be the output of the find_matchup search i.e. a \
granule-links.csv file''')
    
    parser.add_argument('--fofile', nargs=1, type=str, required=False, help='''\
      file path for the full output file which contains the full list of stations matched to \
      granule names and urls. This will have duplicates of the granule info, but a unique \
      station list.''')

    parser.add_argument('--uofile', nargs=1, type=str, required=True, help='''\
      file path for the unique output file which contains the unique list of granule names and urls.''')

    args=parser.parse_args()
    dict_args=vars(args)
    filepath = dict_args['ifile'][0]
    if not args.fofile:
        dict_args['fofile'] = dict_args['ifile']
    
    grandf = pd.read_csv(filepath,names=['lat','lon','datetimes','station','granurls'])
    
    grandf['granurls'] = grandf['granurls'].apply(lambda x : urledit(x))
    
    grandf = grandf.loc[~grandf.granurls.str.contains('_GAC')]
    
    grandf['granid'] = grandf['granurls'].apply(lambda x : getgranname(x))
    
    # The full list of stations matched to granule names and urls (contains duplicate granule info)
    outdf1 = grandf[['station','granid','granurls']]
    outdf1.to_csv(dict_args['fofile'][0],index=False,header=False)
    
    # The unique list of granule names and urls
    outdf2 = outdf1.groupby('granid').head(1)[['granid','granurls']]
    outdf2.to_csv(dict_args['uofile'][0],index=False,header=False)
    
def urledit(urlstring):
    import re
    
    urlstring = urlstring.replace('L2_','L1A_').replace('_OC','')
    file = re.split('/',urlstring)[-1]
    if file[0] in ['A','T','S']:
        urlstring = urlstring.replace('.nc','.bz2')
    
    return urlstring    
    
def getgranname(urlstring):
    import re
    
    return re.search('[A-Z][0-9]{13}',urlstring).group()

if __name__ == "__main__": main()