
def main():
    
    import argparse
    import pandas as pd
    import re
    
    
    parser = argparse.ArgumentParser(description='''\
      In this script, we collect all datarows in the L1a file that map to the same granule. \
      We expand the bounding box to the maximum limits of the said datarows so that all \
      field data mapping to the same granule are included within the final bounding box. \
      Additionally, we then only keep the rows with the unique L1a granules.''')

    parser.add_argument('--L1aGranlinksFile', nargs=1, type=str, required=True, help='''\
      Full path of input file that contains the granule info with L1a extensions. ''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
      File path for the output file which will contain a unique list of L1a download granules\
      with appropriately expanded bounding boxes.''')

    args=parser.parse_args()
    dict_args=vars(args)
    
    filepath = dict_args['L1aGranlinksFile'][0]
    
    df_l1a = pd.read_csv(filepath, names=['station','granid','granurl','wlon','slat','elon','nlat'])
    
    urls = []
    wlons = []
    elons = []
    slats = []
    nlats = []
    
    for url in df_l1a['granurl'].unique():
        curr_df = df_l1a.loc[df_l1a['granurl']==url]
        urls.append(url)
        wlons.append(curr_df['wlon'].min())
        elons.append(curr_df['elon'].max())
        slats.append(curr_df['slat'].min())
        nlats.append(curr_df['nlat'].max())
        
    download_l1as = pd.DataFrame({'granurl':urls,'wlon':wlons,'slat':slats,'elon':elons,'nlat':nlats})
    download_l1as.to_csv(dict_args['ofile'][0], index=False, header=False)
        
    
if __name__ == "__main__": main()