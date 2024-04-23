import pandas as pd
import geopandas as gpd
import glob
import sys
import os
all_files = glob.glob("../../../data/processed/sen/*.csv")

block_grps = gpd.read_file("../../../data/boundary/US_blck_grp_2011.shp")
msa_xls = pd.read_excel("../../../data/datasets_misc/list1_Sep_2018.xls",skiprows=2,skipfooter=4,dtype={"CBSA Code":"str",
                                                                                           "CSA Code":"str",
                                                                                           "FIPS State Code":"str",
                                                                                           "FIPS County Code":"str"
                                                                                          })

bg_with_msa = pd.merge(block_grps,msa_xls,left_on=["STATEFP","COUNTYFP"],right_on=["FIPS State Code","FIPS County Code"],how="inner")
bg_with_msa = bg_with_msa.query("`Metropolitan/Micropolitan Statistical Area` == 'Metropolitan Statistical Area'")
bg_with_msa = bg_with_msa.to_crs("EPSG:4326")

def process_file(f):
    df = pd.read_csv(f,dtype={'id':str,'tweet_created_at_int':str})
    
    print(df.shape,base)
    gdf = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(df.lon,df.lat,crs="EPSG:4326"))
    gdf = gpd.sjoin(gdf,bg_with_msa)
    gdf['geometry'] = gdf['geometry'].to_wkt()
    save_path = f"../../../data/processed/joined/{base}"
    print(save_path)
    gdf.to_csv(save_path,index=False)

for f in all_files:
    base = os.path.basename(f)
    save_path = f"../../../data/processed/joined/{base[:-4]}.geojson"
    #check if file exist
    if not os.path.exists(save_path):
        print(f)
        process_file(f)