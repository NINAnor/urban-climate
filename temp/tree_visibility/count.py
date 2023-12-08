# Calculate Count Statistics for Tree Visibility
# n_trees = number of trees in the district
# n_bldg = number of buildings in the district
# n_res_bldg = number of residential buildings in the district
# n_res_bldg_near_gs = number of residential buildings near green spaces in the district
# n_bldg_near_trees = number of res buidlings with more than 3 trees within a 15m radius

import os

import duckdb
import geopandas as gpd
import pandas as pd

# convert geom from binary to shapely
from shapely.wkb import loads


def get_parquet_dict(district_number, interim_dir):
    file_names = [
        f"districts_{district_number}",
        f"bldg_{district_number}",
        f"res_bldg_{district_number}",
        f"tree_crowns_{district_number}",
    ]

    parquet_dict = {
        name: os.path.join(interim_dir, "per_district", "parquet", f"{name}.parquet")
        for name in file_names
    }

    return parquet_dict


def load_parquet_to_duckdb(input_parquet, output_table, db_path):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # load spatial extension
        con.install_extension("spatial")
        con.load_extension("spatial")

        # check if table exists
        table_exists = con.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{output_table}';"
        ).fetchone()
        if table_exists:
            print(f"Table {output_table} already exists. Skipping.")
            return

        # add table
        if os.path.exists(input_parquet):
            con.execute(
                f"""
                CREATE TABLE {output_table} AS 
                SELECT * 
                FROM parquet_scan('{input_parquet}')
                """
            )
            print(f"Loaded table: {output_table}")
            
            # remove leading zeros from 'grunnkretsnummer' column
            con.execute(f"""
                UPDATE {output_table}
                SET grunnkretsnummer = TRIM(LEADING '0' FROM grunnkretsnummer)
            """)
            
            # if col_name district_code exists, rename to grunnkretsnummer
            columns = con.execute(f"PRAGMA table_info({output_table})").fetch_df()['name'].tolist()
            if 'district_code' in columns:
                # remove grunnkretsnummer column
                con.execute(f"""
                    ALTER TABLE {output_table}
                    DROP COLUMN grunnkretsnummer
                """)
                # rename district_code to grunnkretsnummer
                con.execute(f"""
                    ALTER TABLE {output_table}
                    RENAME COLUMN district_code TO grunnkretsnummer
                """)
        else:
            print(f"File {input_parquet} does not exist. Skipping.")

def check_column_in_tables(db_path):
    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Get the list of all tables in the database
        tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetch_df()['name'].tolist()

        for table in tables:
            # Check if 'grunnkretsnummer' column exists in the table
            columns = con.execute(f"PRAGMA table_info({table})").fetch_df()['name'].tolist()
            if 'grunnkretsnummer' in columns:
                # Check if 'grunnkretsnummer' column is filled with values with length 8
                df = pd.read_sql(f"SELECT grunnkretsnummer FROM {table} WHERE LENGTH(grunnkretsnummer) = 7", con)
                if not df.empty:
                    print(f"Table {table} contains 'grunnkretsnummer' column with values of length 8.")
                else:
                    print(f"Table {table} contains 'grunnkretsnummer' column but it does not have values of length 8.")
            else:
                print(f"Table {table} does not contain 'grunnkretsnummer' column.")

def print_duckdb_info(db_path):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # load spatial extension
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Get table names
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        print("Tables:", tables)

        # Print column names of district table
        columns = con.execute("PRAGMA table_info(districts);").fetchall()
        print("Columns of districts table:", columns)

def remove_duckdb_database(db_path):
    if os.path.exists(db_path):
        os.remove(db_path)
    else:
        print(f"The file {db_path} does not exist")


def remove_all_except_two(db_path, table_a, table_b):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # Get table names
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        # Drop each table that is not 'districts'
        for table in tables:
            table_name = table[0]
            if table_name != table_a and table_name != table_b:
                con.execute(f"DROP TABLE {table_name};")


def add_columns(db_path, district_number):
    with duckdb.connect(database=db_path, read_only=False) as con:
        columns = [
            "n_trees",
            "n_bldg",
            "n_res_bldg",
            "n_res_bldg_near_gs",
            "perc_near_gs",
            "n_bldg_near_trees",
            "perc_near_trees",
            "a_crown",
            "perc_crown",
        ]
        existing_columns = [
            column[1]
            for column in con.execute(f"PRAGMA table_info(districts_{district_number});").fetchall()
        ]

        for column in columns:
            if column in existing_columns:
                print(f"Column {column} already exists. Skipping.")
                continue

            con.execute(f"ALTER TABLE districts_{district_number} ADD COLUMN {column} INTEGER")


def n_trees(db_path, district_number, col_join='grunnkretsnummer'):
    # based on districtnumber (NOT GEOMETRY)

    with duckdb.connect(database=db_path, read_only=False) as con:
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_trees = (
                SELECT COUNT(*)
                FROM tree_crowns_{district_number}
                WHERE districts_{district_number}.{col_join} = tree_crowns_{district_number}.{col_join}
                )
            """
        )
        count = con.execute(f"SELECT n_trees FROM districts_{district_number}").fetchall()
        return count

def n_bldg(db_path, district_number, col_join='grunnkretsnummer'):
    # based on districtnumber (NOT GEOMETRY)

    with duckdb.connect(database=db_path, read_only=False) as con:
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_bldg = (
                SELECT COUNT(*)
                FROM bldg_{district_number}
                WHERE districts_{district_number}.{col_join} = bldg_{district_number}.{col_join}
                )
            """
        )
        count = con.execute(f"SELECT n_bldg FROM districts_{district_number}").fetchall()
        return count

def n_res_bldg(db_path, district_number, col_join='grunnkretsnummer'):
    # based on districtnumber (NOT GEOMETRY)

    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")
        
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_res_bldg = (
                SELECT COUNT(*)
                FROM res_bldg_{district_number}
                WHERE districts_{district_number}.{col_join} = res_bldg_{district_number}.{col_join}
                )
            """
        )
        count = con.execute(f"SELECT n_res_bldg FROM districts_{district_number}").fetchall()
        return count

def n_res_bldg_near_gs(db_path, district_number, col_join='grunnkretsnummer'):
    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")

        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_res_bldg_near_gs = (
                SELECT COUNT(res_bldg_{district_number}.{col_join})
                FROM res_bldg_{district_number}
                LEFT JOIN green_space
                ON ST_DWithin(
                    ST_GeomFromWKB(res_bldg_{district_number}.geometry), 
                    ST_GeomFromWKB(green_space.geometry), 
                    300.0
                )
                WHERE districts_{district_number}.{col_join} = res_bldg_{district_number}.{col_join}
            )
            """
        )
        count = con.execute(f"SELECT n_res_bldg_near_gs FROM districts_{district_number}").fetchall()
        return count

def n_res_bldg_near_trees(db_path, district_number, col_join='grunnkretsnummer'):
    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")

        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_bldg_near_trees = (
                SELECT res_bldg_count
                FROM (
                    SELECT 
                        {col_join}, 
                        COUNT({col_join}) AS res_bldg_count
                    FROM (
                        SELECT 
                            res_bldg_{district_number}.{col_join}, 
                            COUNT(tree_crowns_{district_number}.{col_join}) AS tree_count
                        FROM 
                            res_bldg_{district_number}
                        LEFT JOIN 
                            tree_crowns_{district_number}
                        ON 
                            ST_DWithin(
                                ST_GeomFromWKB(res_bldg_{district_number}.geometry), 
                                ST_GeomFromWKB(tree_crowns_{district_number}.geometry), 
                                15.0
                            )
                        GROUP BY 
                            res_bldg_{district_number}.{col_join}
                    ) AS subquery
                    WHERE 
                        tree_count > 3
                    GROUP BY 
                        {col_join}
                ) AS subquery2
                WHERE districts_{district_number}.{col_join} = subquery2.{col_join}
            )
            """
        )
        
        count = con.execute(f"SELECT n_bldg_near_trees FROM districts_{district_number}").fetchall()
        return count

def a_crown(db_path, district_number, col_join='grunnkretsnummer'):
    # sum crown area for all trees in the district
    with duckdb.connect(database=db_path, read_only=False) as con:

        con.install_extension("spatial")
        con.load_extension("spatial")
        
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET a_crown = (
                SELECT SUM(ST_Area(ST_GeomFromWKB(tree_crowns_{district_number}.geometry)))
                FROM tree_crowns_{district_number}
                WHERE districts_{district_number}.{col_join} = tree_crowns_{district_number}.{col_join}
                )
            """
        )
        count = con.execute(f"SELECT a_crown FROM districts_{district_number}").fetchall()
        return count
        
def update_table(db_path, district_number):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # if NAN set to 0
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_res_bldg_near_gs = COALESCE(n_res_bldg_near_gs, 0),
                n_bldg = COALESCE(n_bldg, 0),
                n_res_bldg = COALESCE(n_res_bldg, 0),
                n_bldg_near_trees= COALESCE(n_bldg_near_trees, 0)
            """
        )

        # normalize perc_near_gs
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET perc_near_gs = (n_res_bldg_near_gs / n_res_bldg) * 100
            """
        )

        # normalize perc_near_trees
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET perc_near_trees = (n_bldg_near_trees / n_res_bldg) * 100
            """
        )
        
        # PERCENTAGE of tree crown coverage per district 
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET perc_crown = (a_crown / a_clipped) * 100
            """
        )

        # if NAN set to 0 
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET perc_near_gs = COALESCE(perc_near_gs, 0),
                perc_near_trees = COALESCE(perc_near_trees, 0),
                perc_crown = COALESCE(perc_crown, 0)
            """
        )
        
def export_districts_to_gdf(db_path, district_number):
    with duckdb.connect(database=db_path, read_only=False) as con:

        df = pd.read_sql(f"SELECT * FROM districts_{district_number}", con)
        # Convert geometry column from WKB to shapely geometry
        df['geometry'] = df['geometry'].apply(loads, hex=True)
        
        # Convert DataFrame to GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        gdf_sorted = gdf.sort_values(by='grunnkretsnummer', ascending=True)
        gdf_sorted = gdf_sorted.round(2)

    return gdf

def export_all_tables_to_csv(db_path, reporting_dir):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # Get table names
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        # Export each table to csv
        for table in tables:
            table_name = table[0]
            df = pd.read_sql(f"SELECT * FROM {table_name}", con)
            df.to_csv(os.path.join(reporting_dir, f"{table_name}.csv"), index=False)

def main(district_numbers, interim_dir, db_path, reporting_dir):

    # remove duckdb database if it exists

    # add district layer to duckdb
    district_path = os.path.join(interim_dir, f"{municipality}_districts.parquet")
    load_parquet_to_duckdb(district_path, "districts", db_path)

    # add green space layer to duckdb
    green_space_path = os.path.join(interim_dir, f"{municipality}_green_space.parquet")
    load_parquet_to_duckdb(green_space_path, "green_space", db_path)
    
    # remove tables from previous run
    remove_all_except_two(db_path, "districts", "green_space")

    # create empty gdf with crs epsg:25832
    gdfs = []
    
    for district_number in district_numbers:

        print(f"Running.. district number: {district_number}")

        # get parquet files
        parquet_dict = get_parquet_dict(district_number, interim_dir)

        # load parquet files to duckdb
        for table, parquet in parquet_dict.items():
            load_parquet_to_duckdb(
                input_parquet=parquet, output_table=table, db_path=db_path
            )
            
        # add count columns to districts table
        add_columns(db_path, district_number)

        # print duckdb info


        # get count statistics
        COUNT_trees = n_trees(db_path, district_number)
        COUNT_bldg = n_bldg(db_path, district_number)
        COUNT_res_bldg = n_res_bldg(db_path, district_number)
        COUNT_n_res_bldg_near_gs = n_res_bldg_near_gs(db_path, district_number)
        COUNT_res_bldg_near_trees = n_res_bldg_near_trees(db_path, district_number)
        AREA_crown = a_crown(db_path, district_number)
        
        # update table (normalize and fill NANs)
        update_table(db_path, district_number)
        
        # export district to gdf
        gdf = export_districts_to_gdf(db_path,district_number)
        gdfs.append(gdf)
        
        print(gdf[['delomradenummer',
                'grunnkretsnummer', 
                'n_trees',
                'n_bldg',
                'n_res_bldg',
                'n_res_bldg_near_gs',
                'perc_near_gs',
                'n_bldg_near_trees',
                'perc_near_trees',
                'a_crown',
                'perc_crown']])
        
        # save count statistics to csv
        export_all_tables_to_csv(db_path, reporting_dir)

        print(f"Finished running district number: {district_number}")

    # organise gdf 
    # concatenate all GeoDataFrames at once
    gdf_all = pd.concat(gdfs, ignore_index=True)
    gdf_sorted = gdf_all.sort_values(by='grunnkretsnummer', ascending=True)
    gdf_sorted = gdf_sorted.round(2)
    # drop if found
    if 'st_geomfromwkb(geometry)' in gdf_sorted.columns:
        gdf_sorted.drop(columns=['st_geomfromwkb(geometry)'], inplace=True)
    
    # Define the %-bins and labels
    labels = ["no data", "0-25%", "25-50%", "50-75%", "75-100%"]
    bins = pd.IntervalIndex.from_tuples([(-0.01, 25), (25, 50), (50, 75), (75, 100)])
    dict = {
        "nan":"no data", 
        "(-0.01, 25.0]": "0-25%", 
        "(25.0, 50.0]": "25-50%", 
        "(50.0, 75.0]": "50-75%", 
        "(75.0, 100.0]": "75-100%"
        }

    # Near Residential Buildings % Categories
    gdf_sorted['bldg_bins'] = pd.cut(gdf_sorted['perc_near_trees'], bins)
    gdf_sorted['bldg_bins'] = gdf_sorted['bldg_bins'].astype(str)
    gdf_sorted['labels_near_trees'] = gdf_sorted['bldg_bins'].map(dict)

    # Near Green Space % Categories
    gdf_sorted['gs_bins'] = pd.cut(gdf_sorted['perc_near_gs'], bins)
    gdf_sorted['gs_bins'] = gdf_sorted['gs_bins'].astype(str)
    gdf_sorted['labels_near_gs'] = gdf_sorted['gs_bins'].map(dict)

    # Crown Coverate % Categories
    gdf_sorted['crown_bins'] = pd.cut(gdf_sorted['perc_crown'], bins)
    gdf_sorted['crown_bins'] = gdf_sorted['crown_bins'].astype(str)
    gdf_sorted['labels_perc_crown'] = gdf_sorted['crown_bins'].map(dict)

    gdf_sorted.drop(columns=['bldg_bins', 'gs_bins', 'crown_bins'], inplace=True)
    
    # sort columns 
    new_order = ['OBJECTID', 'fylkesnummer', 'fylkesnavn', 'kommunenummer',
        'kommunenavn', 'delomradenummer', 'delomradenavn', 'grunnkretsnummer',
        'grunnkretsnavn', 'kilde_admin', 'kilde_befolkning', 'id_befolkning',
        'year_pop_stat', 'pop_total', 'pop_elderly', 'a_district', 'a_unit',
        'a_clipped', 'n_trees', 'n_bldg', 'n_res_bldg',
        'n_res_bldg_near_gs', 'perc_near_gs', 'labels_near_gs',
        'n_bldg_near_trees', 'perc_near_trees', 'labels_near_trees',
        'a_crown', 'perc_crown','labels_perc_crown',  
        'SHAPE_Length', 'SHAPE_Area', 'geometry']
    
    # Reorder the columns
    gdf_sorted = gdf_sorted[new_order]
    gdf.crs = "EPSG:25832"
    
    # export all districts to csv, geojosn and parquet
    # Export GDF to file 
    filepath = os.path.join(reporting_dir, f"{municipality}_{district_number}_treeVis_stat")

    # Write to .parquet
    gdf_sorted.to_parquet(os.path.join(filepath + '.parquet'))

    # Write to .geojson with coord. ref. system epsg:25832
    gdf_sorted.crs = "EPSG:25832"
    gdf_sorted.to_file(os.path.join(filepath + '.geojson'), driver='GeoJSON')

    # Write to .csv
    gdf_sorted.to_csv(os.path.join(filepath + '.csv'))

if __name__ == "__main__":

    # params
    municipality = "oslo"
    district_numbers = range(30101, 30103)

    # path to data
    root = r"/data/P-Prosjekter2/"
    root_2 = r"/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/"
    data_path = os.path.join(
        root, "152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst", "TEMP"
    )

    raw_dir = os.path.join(data_path, "oslo", "01_raw")
    interim_dir = os.path.join(data_path, "oslo", "02_intermediate")
    reporting_dir = os.path.join(data_path, "oslo", "08_reporting")

    # duckdb database on P-drive
    db_path = os.path.join(interim_dir, "oslo.db")
    main(district_numbers, interim_dir, db_path, reporting_dir)

    # export all tables to csv
    
    # remove duckdb database
    #remove_duckdb_database(db_path)