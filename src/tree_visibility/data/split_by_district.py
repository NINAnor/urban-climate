"""split datasets by area number (grunnkretsnummer/delområdenummer)"""

# TODO move paths to catalog
# TODO move global variables to param file
# TODO move to kedro pipeline

import os

import duckdb
import geopandas as gpd
import leafmap
import pandas as pd
import pyarrow
from shapely.geometry import Point
from shapely.wkb import loads

# set temp dir to network drivve to avoid disk space issues
os.environ[
    "TMPDIR"
] = r"/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst/TEMP"

# PARAMETERS
municipality = "oslo"
TEMP_DIR = os.environ["TMPDIR"]
raw_dir = os.path.join(TEMP_DIR, "oslo", "01_raw")
interim_dir = os.path.join(TEMP_DIR, "oslo", "02_intermediate")
reporting_dir = os.path.join(TEMP_DIR, "oslo", "08_reporting")

# Define the table names
file_names = [
    f"{municipality}_study_area",
    f"{municipality}_districts",
    f"{municipality}_bldg",
    f"{municipality}_res_bldg",
    f"{municipality}_green_space",
    f"{municipality}_open_space",
    f"{municipality}_public_open_space",
    f"{municipality}_private_open_space",
    f"{municipality}_tree_crowns",
]

table_names = [
    "study_area",
    "districts",
    "bldg",
    "res_bldg",
    "green_space",
    "open_space",
    "public_open_space",
    "private_open_space",
    "tree_crowns",
]

district_geojson = os.path.join(interim_dir, f"{municipality}_districts.geojson")

# Define the parquet_dict
parquet_dict = {
    name: os.path.join(interim_dir, f"{name}.parquet") for name in file_names
}

# Check if the parquet files exist, if not convert  to parquet
for key in parquet_dict.keys():
    if os.path.exists(parquet_dict[key]):
        continue
    else:
        # Define the gdf_dict
        gdf_dict = {
            name: gpd.read_file(os.path.join(raw_dir, f"{name}.geojson"))
            for name in file_names
        }

        # Convert GeoDataFrame to Parquet
        for key, gdf in gdf_dict.items():
            gdf.to_parquet(
                path=interim_dir + "/" + key + ".parquet",
                index=None,
                compression="snappy",
            )

# TODO if json file does not exist, try shp file
# gdf = gpd.read_file(os.path.join(raw_dir, "shp", f"{municipality}_public_open_space.shp"))
# gdf.to_parquet(
# path = interim_dir + "/" + f"{municipality}_public_open_space" + ".parquet",
# index = None,
# compression = "snappy"
# )

# LOAD DATA

# load the admin border data (delomrade/grunnkrets/kommune)
districts = gpd.read_file(os.path.join(raw_dir, f"{municipality}_districts.geojson"))
districts = districts.to_crs(epsg=25832)

# export a parquet file for each district (delomrade)
district_list = districts["delomradenummer"].unique()
district_list = sorted(district_list)
# print list of unique delomrade numbers
print(f"Number of districts: {len(district_list)} \n")
print(f"Districts: {district_list}")


# SPLIT ADMIN BORDERS BY DISTRICT
for n in district_list:
    district_number = n
    # print(f"District number: {district_number}")
    district = districts.loc[districts["delomradenummer"] == district_number]

    # if not None display
    if district is not None:
        continue
    else:
        print(f"District {n} is None")

    district.to_parquet(
        path=interim_dir + "/" + f"district_{district_number}" + ".parquet",
        index=None,
        compression="snappy",
    )

# SPLIT OTHER TABLES BY DISTRICT
for district_number in district_list:
    # load district {district_number} from parquet
    con = duckdb.connect(database=":memory:", read_only=False)
    con.install_extension("spatial")
    con.load_extension("spatial")

    # create a duckdb table for the district
    district_path = os.path.join(
        interim_dir, 
        f"district_{district_number}.parquet"
        )
    print(f"District number: {district_number}")

    con.execute(
        f"""
        CREATE TABLE district_{district_number}
        AS SELECT *, ST_GeomFromWKB(geometry) 
        FROM parquet_scan('{district_path}')
        """
    )

    # Load all other tables
    for key, table in zip(parquet_dict.keys(), table_names):
        if table != "districts":
            con.execute(
                f"""
                CREATE TABLE {table} 
                AS SELECT *, ST_GeomFromWKB(geometry) 
                FROM parquet_scan('{parquet_dict[key]}')
                """
            )

    # Spatially clip all other tables to geometry of 
    # 'district_{district_number}' and create a new table 
    # {table}_{district_number} and export it to 
    # {table}_{district_number}.parquet
    for table in table_names:
        if table != "districts":
            con.execute(
                f"""
                CREATE TABLE {table}_{district_number} 
                AS SELECT *, ST_GeomFromWKB(geometry) as geometry
                FROM {table} 
                WHERE ST_Intersects(
                    ST_GeomFromWKB(geometry), 
                    (SELECT ST_GeomFromWKB(geometry) 
                    FROM district_{district_number})
                    )
                """
            )
            con.execute(
                f"""
                COPY (
                    SELECT * 
                    FROM {table}_{district_number}) 
                    TO '{interim_dir}/{table}_{district_number}.parquet' (FORMAT 'parquet')
                """
            )

    con.close()

if __name__ == "__main__":
    pass

# TODO move code to functions and call them from main
