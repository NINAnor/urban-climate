import geopandas as gpd
import os

def get_files_and_tables(geojson_dir, parquet_dir, municipality, table_names):
    """ Create a dictionary {file_name: file_path}
    for all geojson and parquet files in a directory.
    Get table names from parquet file names, without municipality prefix.
    
    Args:
        geojson_dir (str): path to geojson files
        parquet_dir (str): path to parquet files
        municipality (str): municipality name
    
    Returns:
        Tuple of dictionaries (geojson_dict, parquet_dict, table_names)
    """
    
    #{table_name: file_path}
    geojson_dict = {tbl: os.path.join(geojson_dir, f"{municipality}_{tbl}.geojson") for tbl in table_names}
    parquet_dict = {tbl: os.path.join(parquet_dir, f"{municipality}_{tbl}.parquet") for tbl in table_names}
    
    return geojson_dict, parquet_dict, table_names


def process_gdf(gdf, tbl, projection):
    # remove areas smaller than 1m2 for all files except study_area and tree_crowns
    if tbl in ["study_area", "tree_crowns"]:
        print(f"Areas < 1m2 are not removed from {tbl}")
        if gdf.crs.to_epsg() != {projection}:
            print(f"Reprojecting {tbl} to epsg:{projection}")
            gdf = gdf.to_crs(epsg={projection})
        return gdf
    
    else: 
        len_before = len(gdf)
        gdf = gdf[gdf.area > 1]
        len_after = len(gdf)
        print(f"Removed {len_before - len_after} rows from {tbl}")
    
        # if epsg is not {projection}, reproject
        if gdf.crs.to_epsg() != {projection}:
            print(f"Reprojecting {tbl} to epsg:{projection}")
            gdf = gdf.to_crs(epsg={projection})
    
    return gdf
    
def export_by_district(gdf_dict, district_list, col_district, parquet_dir, geojson_dir):
    # for each district
    for number in district_list:
        # for each GeoDataFrame
        for name, gdf in gdf_dict.items():
            
            # convert col_district to int
            number = int(number)
            gdf[col_district] = gdf[col_district].astype(int)
            
            # if col_district not in gdf.columns, continue
            if col_district not in gdf.columns:
                print(f"{name} does not have a {col_district} column")
                continue
            
            print(f"Exporting {name} for district {number}")
            gdf_fan = gdf[gdf[col_district] == number]

            # if gdf_fan is empty, continue
            if gdf_fan.empty:
                print(f"{name} for district {number} is an empty gdf")
                continue
            
            parquet_dir_district = os.path.join(parquet_dir, "per_district")
            geojson_dir_district = os.path.join(geojson_dir, "per_district")
            os.makedirs(parquet_dir_district, exist_ok=True)
            os.makedirs(geojson_dir_district, exist_ok=True)
            parquet_file = os.path.join(parquet_dir_district, f"{name}_{number}.parquet")
            geojson_file = os.path.join(geojson_dir_district, f"{name}_{number}.geojson")
            
            # if parquet file already exists, continue
            if os.path.exists(parquet_file):
                print(f"Parquet file for {name} and district {number} already exists")
                continue
            else:
                gdf_fan.to_parquet(
                    path = parquet_file,
                    index = None, 
                    compression = "snappy"
                )
            
            # if geojson file already exists, continue
            if os.path.exists(geojson_file):
                print(f"Geojson file for {name} and district {number} already exists")
                continue
            else:
                gdf_fan.to_file(
                    geojson_file,
                    driver='GeoJSON'
                )
    return print("Fanning out complete.")

def main(geojson_dir, parquet_dir, municipality, table_names):
    
    print(f"Getting file paths for {municipality}...")
    geojson_dict, parquet_dict, table_names = get_files_and_tables(geojson_dir, parquet_dir, municipality, table_names)
    # get file paths
    print(f"Loaded tables: {table_names}")
    
    # load the district data
    districts = gpd.read_file(geojson_dict["districts"])
    district_list = districts['delomradenummer'].unique()
    district_list = sorted(district_list)

    # print list of unique delomrade numbers 
    print(f"Number of districts: {len(district_list)} \n")
    print(f"Districts: {district_list}")

    # {table_name: gdf}
    gdf_dict = {}
    
    # Check if the parquet files exist, if not continue
    for file_name, tbl in zip(parquet_dict.keys(), table_names):
        parquet_file = parquet_dict[file_name]
        
        # If Parquet file does not exist
        if not os.path.exists(parquet_file):
            print(f"Parquet file for {parquet_file} does not exists.")
            print(f"File is not being fanned out.")
            continue
        
        # Open Parquet file
        gdf = gpd.read_parquet(parquet_file)
        print(f"Loaded file {tbl}")
        print(f"Number of rows: {len(gdf)}")
        print(f"Columns: {gdf.columns}")
        #print(f"Head: {gdf.head()}")

        
        # add to gdf_dict
        gdf_dict[tbl] = gdf 
        
    print(gdf_dict.keys())

    export_by_district(gdf_dict, district_list, 'delomradenummer', parquet_dir, geojson_dir)
   

if __name__ == "__main__":

    # params
    municipality = "baerum"

    # path to data
    root = r"/data/P-Prosjekter2/"
    root_2 = r"/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/"
    data_path = os.path.join(
        root_2, 
        "152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst", 
        "data",
        municipality,
        "general"
        )

    geojson_dir = os.path.join(data_path, "GEOJSON", "cleaned")
    parquet_dir = os.path.join(data_path, "PARQUET")

    # Define the table names
    file_names = [
        f"{municipality}_districts",
        f"{municipality}_bldg",
        f"{municipality}_res_bldg",
        f"{municipality}_tree_crowns"
        ]

    table_names = [
        "districts", 
        "bldg", 
        "res_bldg", 
        "tree_crowns"
        ]
    
    main(geojson_dir, parquet_dir, municipality, table_names)
    
    