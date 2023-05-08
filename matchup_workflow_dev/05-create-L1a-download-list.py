
def main():
    
    import argparse
    import pandas as pd
    import re
    
    
    parser = argparse.ArgumentParser(description='''\
      In this script, we collect all datarows in the L1a file that map to the same granule. \
      We expand the bounding box to the maximum limits of the said datarows so that all \
      field data mapping to the same granule are included within the final bounding box. \
      Additionally, we then only keep the rows with the unique L1a granules. We output multiple files:\
       one compiled file of all unique granule urls, and then one file each for the separate satellites.\
        Note that this script was based on searching six satellites: seawifs, aqua, terra, snpp, jpss1, and jpss2. \
        If user decides to search for another satellite, user MUST input satellite info into satellite \
        name dictionary defined within this script.''')

    parser.add_argument('--L1aGranlinksFile', nargs=1, type=str, required=True, help='''\
      Full path of input file that contains the granule info with L1a extensions. ''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
      File path for the output file which will contain a unique list of L1a download granules\
      with appropriately expanded bounding boxes.''')

    args=parser.parse_args()
    dict_args=vars(args)
    
    filepath = dict_args['L1aGranlinksFile'][0]
    
    df_l1a = pd.read_csv(filepath, names=['station','granid','granurl','wlon','slat','elon','nlat'])
    
    gids = []
    urls = []
    wlons = []
    elons = []
    slats = []
    nlats = []
    
    for url in df_l1a['granurl'].unique():
        curr_df = df_l1a.loc[df_l1a['granurl']==url]
        gids.append(curr_df['granid'].iloc[0])
        urls.append(url)
        wlons.append(curr_df['wlon'].min())
        elons.append(curr_df['elon'].max())
        slats.append(curr_df['slat'].min())
        nlats.append(curr_df['nlat'].max())
    
    unique_granules_df = pd.DataFrame({'granid':gids,'granurl':urls,'wlon':wlons,'slat':slats,'elon':elons,'nlat':nlats})
    unique_granules_df.to_csv(dict_args['ofile'][0], index=False, header=False)
    
    # Separate output file into individual files by satellite:
    satellite_names = {'S':'seawifs','A':'aqua','T':'terra','VS':'snpp','V1J':'jpss1','V2J':'jpss2'}
    
    for key in satellite_names:
        if 'V' not in key:
            sat_df = unique_granules_df.loc[unique_granules_df['granid'].str[0]==key]
            sat_df.to_csv(dict_args['ofile'][0][0:-4]+'-'+satellite_names[key]+'.csv', index=False, header=False)
        elif key=='VS':
            sat_df = unique_granules_df.loc[unique_granules_df['granid'].str[0:2]==key]
            sat_df.to_csv(dict_args['ofile'][0][0:-4]+'-'+satellite_names[key]+'.csv', index=False, header=False)
        else:
            sat_df = unique_granules_df.loc[unique_granules_df['granid'].str[0:3]==key]
            sat_df.to_csv(dict_args['ofile'][0][0:-4]+'-'+satellite_names[key]+'.csv', index=False, header=False)
        
    
if __name__ == "__main__": main()