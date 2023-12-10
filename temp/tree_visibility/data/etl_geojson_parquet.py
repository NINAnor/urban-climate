""" Convert geojson to parquet """

import os
import geopandas as gpd

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

def preprocess_geojson(tbl, gdf, projection):
    """ Preprocess geojson files.
    - remove areas smaller than 1m2 
        (except for study_area and tree_crowns)
    - reproject to desired projection
    
    Args:
        tbl (str): table name
        gdf (gpd.GeoDataFrame): geojson as geopandas dataframe
        projection (str): desired projection
    
    Returns:
        gdf (gpd.GeoDataFrame): preprocessed geojson as geopandas dataframe
    """
    if tbl in ["study_area", "tree_crowns", "registered_trees"]:
        print(f"Areas < 1m2 are not removed from {tbl}")
        if gdf.crs.to_epsg() != projection:
            print(f"Reprojecting {tbl} to epsg:{projection}")
            gdf = gdf.to_crs(epsg=projection)
        return gdf
    
    else: 
        len_before = len(gdf)
        gdf = gdf[gdf.area > 1]
        len_after = len(gdf)
        print(f"Removed {len_before - len_after} rows from {tbl}")
    
        # if epsg is not %projection%, reproject
        if gdf.crs.to_epsg() != projection:
            print(f"Reprojecting {tbl} to epsg:{projection}")
            gdf = gdf.to_crs(epsg=projection)
    
    return gdf



def load_clean(file_dict, projection, col_district="grunnkretsnummer"):
    """ Load and clean files (geojson/parquet) to gdf_dict {table_name: gdf}

    Args:
        file_dict (dict): _description_
        projection (str): _description_
        col_district (str, optional): _description_. Defaults to "grunnkretsnummer".

    Returns:
        gdf_dict: dict of geopandas dataframes {table_name: gdf}
    """
    
    gdf_dict = {}
    for tbl, filepath in file_dict.items():        
        # if file already exists in cleaned folder, skip 
        if os.path.exists(os.path.join(geojson_dir, "cleaned", f"{municipality}_{tbl}.geojson")):
            print(f"File <{tbl}> already exists in cleaned folder, skip cleaning...")
            print(f"Loading {tbl} from cleaned folder...")
            gdf = gpd.read_file(os.path.join(geojson_dir, "cleaned", f"{municipality}_{tbl}.geojson"))

        # if file does not exist, clean and export
        if not os.path.exists(filepath):
            print(f"File <{tbl}> does not exist, start cleaning...")
            gdf = gpd.read_file(filepath)
            # remove small areas and reproject
            gdf = preprocess_geojson(tbl, gdf, projection)
            
            # special cleaning for tree_crowns
            # rename cols to match parquet cols
            if tbl == "tree_crowns":
                gdf.rename(columns={"district_code": "grunnkretsnummer", "area_code": "delomradenummer"}, inplace=True)
                print(f"Cleaned columns for {tbl}: {gdf.columns}")
        
            # sort by district number
            if col_district in gdf.columns:
                gdf.sort_values(by=col_district, inplace=True)
                print(f"Sorted {tbl} by {col_district}")
            
        # add to dict {table_name: gdf}
        gdf_dict[tbl] = gdf

    return gdf_dict

def export_geojson(gdf_dict, output_dir, municipality, projection):
    """ Export cleaned gdfs to geojson."""
    for tbl, gdf in gdf_dict.items():
        # if file already exists in cleaned folder, skip
        filepath = os.path.join(output_dir, f"{municipality}_{tbl}.geojson")
        if not os.path.exists(os.path.join(output_dir, f"{municipality}_{tbl}.geojson")):
            # ensure coord. 
            gdf.crs = f"EPSG:{projection}"
            gdf.to_file(filepath, driver="GeoJSON")
    return print(f"Exported cleaned {tbl} to {filepath}")
    
def export_parquet(gdf_dict, output_dir, municipality, projection):
    """ Export cleaned gdfs files to parquet."""
    for tbl, gdf in gdf_dict.items():
        # if file already exists in cleaned folder, skip
        filepath = os.path.join(output_dir, f"{municipality}_{tbl}.parquet")
        if not os.path.exists(os.path.join(output_dir, f"{municipality}_{tbl}.parquet")):
            # ensure coord. 
            gdf.crs = f"EPSG:{projection}"
            gdf.to_parquet(filepath)
    return print(f"Exported cleaned {tbl} to {filepath}")

# clean for excel export
def clean_for_excel(gdf, tbl, col_id = "crown_id"):
    """ Clean tree_crowns and registered_trees for excel export."""
    
    # convert gdf to df and remove geometry
    df = gdf.drop(columns="geometry")
    
    # new col ID based on crown_id (remove all non numbers)
    # if col is string (registered_trees), remove all non numbers
    if df[col_id].dtype == "object":
        df["ID"] = df[col_id].str.replace(r"\D", "")
        # convert to numeric
        import pandas as pd
        df["ID"] = pd.to_numeric(df["ID"])
    else:
        df["ID"] = df[col_id]
    
    # sort by ID 
    df_sorted = df.sort_values(by="ID", ascending=True)
    # round all numeric cols (except wgs84_lon and wgs84_lat) to 2 decimals
    numeric_cols = df_sorted.select_dtypes(include="number").columns
   
    numeric_cols = [col for col in numeric_cols if col not in ['wgs84_lon', 'wgs84_lat']]
    # Round these columns to 2 decimal places
    df_round = df_sorted.copy()
    df_round[numeric_cols] = df_round[numeric_cols].round(decimals=2)
    
    # create summary table
    df_summary = df_round.describe().T
    
    return df, df_summary

def export_csv(gdf_dict, output_dir, municipality):
    """ Export cleaned gdfs files to csv."""
    col_id_dict = {
        "districts": "grunnkretsnummer",
        "tree_crowns": "crown_id" if municipality != "oslo" else "OBJECTID",
        "registered_trees": "tree_id" if municipality != "oslo" else "TREE_ID"
    }

    for tbl, gdf in gdf_dict.items():
        # if file already exists in cleaned folder, skip
        filepath = os.path.join(output_dir, f"{municipality}_{tbl}.csv")
        filepath_summary = os.path.join(output_dir, f"{municipality}_{tbl}_summary.csv")

        if not os.path.exists(filepath) and tbl in col_id_dict:
            df, df_summary = clean_for_excel(gdf, tbl, col_id_dict[tbl])
            df.to_csv(filepath, encoding="utf-8")
            df_summary.to_csv(filepath_summary, encoding="utf-8")
            print(f"Exported cleaned {tbl}.")

def export_excel(gdf_dict, output_dir, municipality):
    """ Export cleaned gdfs files to csv."""
    col_id_dict = {
        "districts": "grunnkretsnummer",
        "tree_crowns": "crown_id" if municipality != "oslo" else "OBJECTID",
        "registered_trees": "tree_id" if municipality != "oslo" else "TREE_ID"
    }

    for tbl, gdf in gdf_dict.items():
        # if file already exists in cleaned folder, skip
        filepath = os.path.join(output_dir, f"{municipality}_{tbl}.xlsx")
        filepath_summary = os.path.join(output_dir, f"{municipality}_{tbl}_summary.xlsx")

        if not os.path.exists(filepath) and tbl in col_id_dict:
            df, df_summary = clean_for_excel(gdf, tbl, col_id_dict[tbl])
            df.to_excel(filepath)
            df_summary.to_excel(filepath_summary)
            print(f"Exported cleaned {tbl}.")

def main(geojson_dir, parquet_dir, csv_dir, excel_dir, municipality, table_names, projection, input_format="geojson"):
    """ Load and clean geojson/parquet files to gdf_dict {table_name: gdf}.
    Export cleaned gdf_dict to geojson and parquet files.

    Args:
        geojson_dir (str): _description_
        parquet_dir (str): _description_
        municipality (str): _description_
        projection (int): _description_
        input_format (str, optional): _description_. Defaults to "geojson".
    
    Returns:
        None
    """
    
    print(f"Getting file paths for {municipality}...")
    geojson_dict, parquet_dict, table_names = get_files_and_tables(geojson_dir, parquet_dir, municipality, table_names)
    # get file paths
    print(f"Loaded tables: {table_names}")

    print(f"Loading and cleaning {input_format} files...")
    if input_format == "geojson":
        # load and clean .geojson files to gdf_dict {table_name: gdf}
        gdf_dict = load_clean(
            file_dict= geojson_dict,
            projection= projection, 
            col_district="grunnkretsnummer"
        )
        
    elif input_format == "parquet":
        # load and clean .parquet files to gdf_dict {table_name: gdf}
        gdf_dict = load_clean(
            file_dict= parquet_dict,
            projection= projection,
            col_district="grunnkretsnummer"
        )    
    
    print(gdf_dict.keys())              
    
    print(f"Exporting cleaned files to geojson...")    
    output_dir_geojson = os.path.join(geojson_dir, "cleaned")
    os.makedirs(output_dir_geojson, exist_ok=True)
    export_geojson(
        gdf_dict = gdf_dict,
        output_dir = output_dir_geojson,
        municipality= municipality, 
        projection= projection
        )
    
    print(f"Exporting cleaned files to parquet...")    
    os.makedirs(parquet_dir, exist_ok=True)
    export_parquet(
        gdf_dict = gdf_dict,
        output_dir = parquet_dir,
        municipality= municipality, 
        projection= projection
        )
    
    print(f"Exporting cleaned files to CSV...")
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(excel_dir, exist_ok=True)
    
    #subset gdf_dict to only include the following tables
    gdf_dict_csv = {tbl: gdf for tbl, gdf in gdf_dict.items() if tbl in ["districts", "tree_crowns", "registered_trees"]}
    
    # export
    export_csv(
        gdf_dict = gdf_dict_csv,
        output_dir = csv_dir,
        municipality= municipality, 
        )
    export_excel(
        gdf_dict = gdf_dict_csv,
        output_dir = excel_dir,
        municipality= municipality,
    )
    
    return None


if __name__ == "__main__":
    
    # params
    municipality = "oslo"

    # path to data
    root = r"/data/P-Prosjekter2/"
    root_2 = r"/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/"
    root_3 = r"P:/"
    data_path = os.path.join(
        root, 
        "152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst", 
        "data",
        municipality,
        "general"
        )

    geojson_dir = os.path.join(data_path, "GEOJSON")
    parquet_dir = os.path.join(data_path, "PARQUET")
    csv_dir = os.path.join(data_path, "CSV")
    excel_dir = os.path.join(data_path, "EXCEL")
    
    table_names = [
        #"study_area", 
        "districts", 
        #"bldg", 
        #"res_bldg", 
        #"green_space",
        #"open_space", 
        #"public_open_space", 
        #"private_open_space", 
        #"registered_trees",
        "tree_crowns"
        #"education_property",  # only oslo
    ]

    epsg = 25832
    
    # LOAD, CLEAN AND EXPORT FILES
    main(
        geojson_dir= geojson_dir,
        parquet_dir= parquet_dir,
        csv_dir= csv_dir,
        excel_dir= excel_dir,
        table_names= table_names,
        municipality= municipality,
        projection= epsg,
        input_format="parquet"
        )
    
    print("Done!")
