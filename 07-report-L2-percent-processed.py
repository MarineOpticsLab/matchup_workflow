def main():
    
    import argparse
    import pandas as pd
    import numpy as np
    import os
    
    
    parser = argparse.ArgumentParser(description='''\
      This script reports the percentage of satellite files successfully processed to L2. It prints the percentages to the job submission output file.''')
    
    parser.add_argument('--downloadUrlsFile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of csv containing unique L1a granules for all satellites.''')
    
    parser.add_argument('--satelliteFileDirectory', nargs=1, type=str, required=True, help='''\
    Full path and name of parent directory containing satellite specific subdirectories in which L2 files have been saved.''')
    
    args=parser.parse_args()
    dict_args=vars(args)
    
    urls_fp = dict_args['downloadUrlsFile'][0]
    satDir = dict_args['satelliteFileDirectory'][0]
    
    urls = pd.read_csv(urls_fp, names=['granid','granurl','wlon','slat','elon','nlat'])
    
    num_urls = len(urls)
    
    unique_sats = np.unique([gid[0:-13] for gid in urls['granid']])
    
    l2_files = []
    for root, dirs, files in os.walk(satDir):
        for file in files:
            if file.endswith('.L2'):
                l2_files.append(file)
                
    tot_percent_processed = len(l2_files)*100/num_urls
    
    print('Total percentage of satellite files that successfully processed to L2: ', tot_percent_processed)
    
    for sat in unique_sats:
        num_sat_urls = len(urls.loc[urls['granid'].str.contains(sat)])
        sat_l2_files = [f for f in l2_files if sat in f]
        sat_percent_processed = len(sat_l2_files)*100/num_sat_urls
        print('Percentage of ', sat, ' files that successfully processed to L2: ', sat_percent_processed)
        
if __name__ == "__main__": main()