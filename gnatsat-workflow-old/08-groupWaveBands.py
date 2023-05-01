## This script groups the various wavelengths measured by the four satellites in gnatsat (seawifs,
## viirs, aqua, and terra) into 10 color bands. It reports the nanmean of the grouped wavelengths measurements.
##
## b01: 410,412
## b02: 443
## b03: 469
## b04: 486, 488, 490
## b05: 510
## b06: 531
## b07: 547, 551, 555
## b08: 645
## b09: 667, 670, 671
## b10: 678

def main():
    
    import pandas as pd
    import numpy as np
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script groups wavelengths from different satellites into color bands.''')
    
    parser.add_argument('--matchupDf', nargs=1, type=str, required=True, help='''\
    Full path and name of matchup dataframe. Include extension.csv.  Note that some column names/ satellite products are hard coded in. If the satellite processing changes, this script should be updated.''')
    
    parser.add_argument('--matchupBandDf', nargs=1, type=str, required=True, help='''\
    Full path and name of where to save the matchup dataframe with color bands added in. Include csv extension.''')
    
    args = parser.parse_args()
    dict_args = vars(args)
    
    df = pd.read_csv(dict_args['matchupDf'][0])
    
    wvlProducts = {'rrs':['Rrs_',''],'rhos':['rhos_',''],'rhot':['rhot_',''],'adg':['adg_','giop_'],'a':['a_','giop_'],'aph':['aph_','giop_'],'bb':['bb_','giop_']}
    statistics = ['mean','median','stdev','filtered_mean','filtered_stdev','grid_size','valid_pixel_count','filtered_pixel_count','flag']
    bandDict = {}
    
    for prod in wvlProducts:
        prefix = wvlProducts[prod][0]
        suffix = wvlProducts[prod][1]
        
        for stat in statistics:
            
            bandDict[prefix+'b01_'+suffix+stat] = [np.nanmean([x410,x412]) if any(~np.isnan([x410,x412])) else np.nan for x410,x412 in zip(df[prefix+'410_'+suffix+stat], df[prefix+'412_'+suffix+stat])]
            bandDict[prefix+'b02_'+suffix+stat] = [element for element in df[prefix+'443_'+suffix+stat]]
            bandDict[prefix+'b03_'+suffix+stat] = [element for element in df[prefix+'469_'+suffix+stat]]
            bandDict[prefix+'b04_'+suffix+stat] = [np.nanmean([x486,x488,x490]) if any(~np.isnan([x486,x488,x490])) else np.nan for x486,x488,x490 in zip(df[prefix+'486_'+suffix+stat],df[prefix+'488_'+suffix+stat],df[prefix+'490_'+suffix+stat])]
            bandDict[prefix+'b05_'+suffix+stat] = [element for element in df[prefix+'510_'+suffix+stat]]
            bandDict[prefix+'b06_'+suffix+stat] = [element for element in df[prefix+'531_'+suffix+stat]]
            bandDict[prefix+'b07_'+suffix+stat] = [np.nanmean([x547,x551,x555]) if any(~np.isnan([x547,x551,x555])) else np.nan for x547,x551,x555 in zip(df[prefix+'547_'+suffix+stat],df[prefix+'551_'+suffix+stat],df[prefix+'555_'+suffix+stat])]
            bandDict[prefix+'b08_'+suffix+stat] = [element for element in df[prefix+'645_'+suffix+stat]]
            bandDict[prefix+'b09_'+suffix+stat] = [np.nanmean([x667,x670,x671]) if any(~np.isnan([x667,x670,x671])) else np.nan for x667,x670,x671 in zip(df[prefix+'667_'+suffix+stat],df[prefix+'670_'+suffix+stat],df[prefix+'671_'+suffix+stat])]
            bandDict[prefix+'b10_'+suffix+stat] = [element for element in df[prefix+'678_'+suffix+stat]]

    bandOnlyDf = pd.DataFrame(bandDict)
    matchupBandDf = pd.concat([df.reset_index(drop=True), bandOnlyDf.reset_index(drop=True)], axis=1)
    
    matchupBandDf.to_csv(dict_args['matchupBandDf'][0], index=False)
    
if __name__ == "__main__": main()