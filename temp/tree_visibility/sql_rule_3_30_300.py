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

def get_parquet_dict(district_number, parquet_dir):
    file_names = [
        f"districts_{district_number}",
        f"bldg_{district_number}",
        f"res_bldg_{district_number}",
        f"tree_crowns_{district_number}",
    ]

    parquet_dict = {
        name: os.path.join(parquet_dir, "per_district", f"{name}.parquet")
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
            
            # get columns
            columns = (
                con.execute(f"PRAGMA table_info({output_table})")
                .fetch_df()["name"]
                .tolist()
            )

            # if col exists remove leading zeros from 'grunnkretsnummer' column
            if "grunnkretsnummer" in columns:
                con.execute(
                    f"""
                    UPDATE {output_table}
                    SET grunnkretsnummer = TRIM(LEADING '0' FROM grunnkretsnummer)
                """
                )

            # if col_name district_code exists, rename to grunnkretsnummer
            if "district_code" in columns:
                # remove grunnkretsnummer column
                con.execute(
                    f"""
                    ALTER TABLE {output_table}
                    DROP COLUMN grunnkretsnummer
                """
                )
                # rename district_code to grunnkretsnummer
                con.execute(
                    f"""
                    ALTER TABLE {output_table}
                    RENAME COLUMN district_code TO grunnkretsnummer
                """
                )
        else:
            print(f"File {input_parquet} does not exist. Skipping.")
            # creaet empty table with col "grunnkretsnumer and delomradenummer type int"
            con.execute(
                f"""
                CREATE TABLE {output_table} (
                    grunnkretsnummer INT,
                    delomradenummer INT,
                    geometry GEOMETRY
                )
            """
            )
    return


def check_column_in_tables(db_path):
    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Get the list of all tables in the database
        tables = (
            con.execute("SELECT name FROM sqlite_master WHERE type='table'")
            .fetch_df()["name"]
            .tolist()
        )

        for table in tables:
            # Check if 'grunnkretsnummer' column exists in the table
            columns = (
                con.execute(f"PRAGMA table_info({table})").fetch_df()["name"].tolist()
            )
            if "grunnkretsnummer" in columns:
                # Check if 'grunnkretsnummer' column is filled with values with length 8
                df = pd.read_sql(
                    f"SELECT grunnkretsnummer FROM {table} WHERE LENGTH(grunnkretsnummer) = 7",
                    con,
                )
                if not df.empty:
                    print(
                        f"Table {table} contains 'grunnkretsnummer' column with values of length 8."
                    )
                else:
                    print(
                        f"Table {table} contains 'grunnkretsnummer' column but it does not have values of length 8."
                    )
            else:
                print(f"Table {table} does not contain 'grunnkretsnummer' column.")
    return


def print_duckdb_info(db_path, district_number):
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
        columns = con.execute(f"PRAGMA table_info(districts_{district_number});").fetchall()
        print("Columns of districts table:", columns)
    return


def remove_duckdb_database(db_path):
    if os.path.exists(db_path):
        os.remove(db_path)
    else:
        print(f"The file {db_path} does not exist")
    return


def remove_all_except_two(db_path, table_a, table_b, table_c):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # Get table names
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        # Drop each table that is not 'districts'
        for table in tables:
            table_name = table[0]
            if table_name != table_a and table_name != table_b and table_name != table_c:
                con.execute(f"DROP TABLE {table_name};")
    return


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
            for column in con.execute(
                f"PRAGMA table_info(districts_{district_number});"
            ).fetchall()
        ]

        for column in columns:
            if column in existing_columns:
                print(f"Column {column} already exists. Skipping.")
                continue

            con.execute(
                f"ALTER TABLE districts_{district_number} ADD COLUMN {column} INTEGER"
            )

    # add column n_green_spaces and n_trees to res_bldg table
    with duckdb.connect(database=db_path, read_only=False) as con:
        columns = ["n_green_spaces", "n_trees"]
        existing_columns = [
            column[1]
            for column in con.execute(
                f"PRAGMA table_info(res_bldg_{district_number});"
            ).fetchall()
        ]

        for column in columns:
            if column in existing_columns:
                print(f"Column {column} already exists. Skipping.")
                continue

            con.execute(
                f"ALTER TABLE res_bldg_{district_number} ADD COLUMN {column} INTEGER"
            )

    return


def n_trees(db_path, district_number, col_join="grunnkretsnummer"):
    # based on ATTRIBUTE VALUE
    # count of all trees in the district

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
        count = con.execute(
            f"SELECT n_trees FROM districts_{district_number}"
        ).fetchall()
    return count


def n_bldg(db_path, district_number, col_join="grunnkretsnummer"):
    # based on ATTRIBUTE VALUE
    # count of all buildings in the district

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
        count = con.execute(
            f"SELECT n_bldg FROM districts_{district_number}"
        ).fetchall()
    return count


def n_res_bldg(db_path, district_number, col_join="grunnkretsnummer"):
    # based on ATTRIBUTE VALUE
    # count of all residential buildings in the district

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
        count = con.execute(
            f"SELECT n_res_bldg FROM districts_{district_number}"
        ).fetchall()
    return count


def n_green_spaces(db_path, district_number):
    # based on GEOMETRY
    # if there is a green spaces within 300 m distance of a res bldg, set n_green_spaces to 1
    # if there is no green space within 300 m distance of a res bldg, set n_green_spaces to 0
    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")

        con.execute(
            f"""
            UPDATE res_bldg_{district_number}
            SET n_green_spaces = (
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM green_space
                        WHERE green_space.geometry IS NOT NULL 
                        AND ST_DWithin(ST_GeomFromWKB(res_bldg_{district_number}.geometry), ST_GeomFromWKB(green_space.geometry), 300)
                    ) THEN 1
                    ELSE 0
                END
            )
            """
        )
        count = con.execute(
            f"SELECT n_green_spaces FROM res_bldg_{district_number}"
        ).fetchall()
    return


def n_res_bldg_near_gs(db_path, district_number, col_join="grunnkretsnummer"):
    # based on ATTRIBUTE VALUE
    # count all values =1 in n_green_spaces column in res_bldg table
    # and add to districts table based on districtnumber

    with duckdb.connect(database=db_path, read_only=False) as con:
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_res_bldg_near_gs = (
                SELECT COUNT(*)
                FROM res_bldg_{district_number}
                WHERE n_green_spaces >= 1 AND districts_{district_number}.{col_join} = res_bldg_{district_number}.{col_join}
                )
            """
        )
        count = con.execute(
            f"SELECT n_res_bldg_near_gs FROM districts_{district_number}"
        ).fetchall()

    return count

def n_near_trees(db_path, district_number):
    # based on GEOMETRY
    # count all trees within 15m distance of a res bldg
    # if count >= 3, set n_trees to 1
    # if count < 3, set n_trees to 0
    with duckdb.connect(database=db_path, read_only=False) as con:
        con.install_extension("spatial")
        con.load_extension("spatial")

        con.execute(
            f"""
            UPDATE res_bldg_{district_number}
            SET n_trees = (
                CASE
                    WHEN (
                        SELECT COUNT(*)
                        FROM tree_crowns
                        WHERE tree_crowns.geometry IS NOT NULL 
                        AND ST_DWithin(ST_GeomFromWKB(res_bldg_{district_number}.geometry), ST_GeomFromWKB(tree_crowns.geometry), 15)
                    ) >= 3 THEN 1
                    ELSE 0
                END
            )
            """
        )
        count = con.execute(
            f"SELECT n_trees FROM res_bldg_{district_number}"
        ).fetchall()
    return count


def n_res_bldg_near_trees(db_path, district_number, col_join="grunnkretsnummer"):
    # based on ATTRIBUTE VALUE
    # count all values =1 in n_trees column in res_bldg table
    # and add to districts table based on districtnumber

    with duckdb.connect(database=db_path, read_only=False) as con:
        con.execute(
            f"""
            UPDATE districts_{district_number}
            SET n_bldg_near_trees = (
                SELECT COUNT(*)
                FROM res_bldg_{district_number}
                WHERE n_trees >= 1 AND districts_{district_number}.{col_join} = res_bldg_{district_number}.{col_join}
                )
            """
        )
        count = con.execute(
            f"SELECT n_bldg_near_trees FROM districts_{district_number}"
        ).fetchall()
    return count


def a_crown(db_path, district_number, col_join="grunnkretsnummer"):
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
        count = con.execute(
            f"SELECT a_crown FROM districts_{district_number}"
        ).fetchall()
    return count


def update_table(db_path, district_number):
    print(f"District number: {district_number}")
    # print district col names 
    with duckdb.connect(database=db_path, read_only=False) as con:
        columns = con.execute(
            f"PRAGMA table_info(districts_{district_number});"
        ).fetchall()
        print("Columns of districts table:", columns)
    
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
    return


def export_districts_to_gdf(db_path, district_number):
    with duckdb.connect(database=db_path, read_only=False) as con:
        df = pd.read_sql(f"SELECT * FROM districts_{district_number}", con)
        # Convert geometry column from WKB to shapely geometry
        df["geometry"] = df["geometry"].apply(loads, hex=True)

        # Convert DataFrame to GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf_sorted = gdf.sort_values(by="grunnkretsnummer", ascending=True)
        gdf_sorted = gdf_sorted.round(2)

    return gdf


def export_all_tables_to_csv(db_path, reporting_dir):
    with duckdb.connect(database=db_path, read_only=False) as con:
        # Get table names
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        # Create folder if not exists
        output_path = os.path.join(reporting_dir, "by_district")
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            
        # Export each table to csv
        for table in tables:
            table_name = table[0]
            df = pd.read_sql(f"SELECT * FROM {table_name}", con)
            df.to_csv(
                os.path.join(output_path, f"{table_name}.csv"),
                index=False,
            )
    return


def concat_all_csvs(geojson_dir, reporting_dir, district_numbers, projection):
    dfs = []
    for district_number in district_numbers:
        df = pd.read_csv(
            os.path.join(
                reporting_dir, "by_district", f"districts_{district_number}.csv"
            )
        )

        # drop geometry columns
        df.drop(columns=["geometry"], inplace=True)
        dfs.append(df)

    # concatenate all DataFrames
    df_all = pd.concat(dfs, ignore_index=True)
    print(df_all.head())
    df_sorted = df_all.sort_values(by="grunnkretsnummer", ascending=True)
    df_sorted = df_sorted.round(2)
    # if starts with 0 remove
    df_sorted["grunnkretsnummer"] = (
        df_sorted["grunnkretsnummer"].astype(str).str.lstrip("0")
    )
    df_sorted["grunnkretsnummer"] = df_sorted["grunnkretsnummer"].astype("int64")

    # load districts table from .geojson to get geometry column
    df_districts = gpd.read_file(
        os.path.join(geojson_dir, f"{municipality}_districts.geojson")
    )

    # convert "grunnkretsnummer" column to int64
    df_districts["grunnkretsnummer"] = (
        df_districts["grunnkretsnummer"].astype(str).str.lstrip("0")
    )
    df_districts["grunnkretsnummer"] = df_districts["grunnkretsnummer"].astype("int64")

    # add geom column from df_districts to df_sorted
    df_merged = pd.merge(
        df_sorted,
        df_districts[["grunnkretsnummer", "geometry"]],
        on="grunnkretsnummer",
        how="left",
    )

    # convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(df_merged, geometry="geometry")
    gdf_sorted = gdf.sort_values(by="grunnkretsnummer", ascending=True)
    gdf_sorted = gdf_sorted.round(2)

    # Define the %-bins and labels
    # labels = ["no data", "0-25%", "25-50%", "50-75%", "75-100%"]
    bins = pd.IntervalIndex.from_tuples([(-0.01, 25), (25, 50), (50, 75), (75, 100)])
    dict = {
        "nan": "no data",
        "(-0.01, 25.0]": "0-25%",
        "(25.0, 50.0]": "25-50%",
        "(50.0, 75.0]": "50-75%",
        "(75.0, 100.0]": "75-100%",
    }

    # Near Residential Buildings % Categories
    gdf_sorted["bldg_bins"] = pd.cut(gdf_sorted["perc_near_trees"], bins)
    gdf_sorted["bldg_bins"] = gdf_sorted["bldg_bins"].astype(str)
    gdf_sorted["labels_near_trees"] = gdf_sorted["bldg_bins"].map(dict)

    # Near Green Space % Categories
    gdf_sorted["gs_bins"] = pd.cut(gdf_sorted["perc_near_gs"], bins)
    gdf_sorted["gs_bins"] = gdf_sorted["gs_bins"].astype(str)
    gdf_sorted["labels_near_gs"] = gdf_sorted["gs_bins"].map(dict)

    # Crown Coverate % Categories
    gdf_sorted["crown_bins"] = pd.cut(gdf_sorted["perc_crown"], bins)
    gdf_sorted["crown_bins"] = gdf_sorted["crown_bins"].astype(str)
    gdf_sorted["labels_perc_crown"] = gdf_sorted["crown_bins"].map(dict)

    gdf_sorted.drop(columns=["bldg_bins", "gs_bins", "crown_bins"], inplace=True)

    # sort columns
    new_order = [
        "OBJECTID",
        "fylkesnummer",
        "fylkesnavn",
        "kommunenummer",
        "kommunenavn",
        "delomradenummer",
        "delomradenavn",
        "grunnkretsnummer",
        "grunnkretsnavn",
        "kilde_admin",
        "kilde_befolkning",
        "id_befolkning",
        "year_pop_stat",
        "pop_total",
        "pop_elderly",
        "a_district",
        "a_unit",
        "a_clipped",
        "n_trees",
        "n_bldg",
        "n_res_bldg",
        "n_res_bldg_near_gs",
        "perc_near_gs",
        "labels_near_gs",
        "n_bldg_near_trees",
        "perc_near_trees",
        "labels_near_trees",
        "a_crown",
        "perc_crown",
        "labels_perc_crown",
        "SHAPE_Length",
        "SHAPE_Area",
        "geometry",
    ]

    # Reorder the columns
    gdf_sorted = gdf_sorted[new_order]
    gdf.crs = f"EPSG:{projection}"

    # export all districts to csv, geojosn and parquet
    # Export GDF to file
    filepath = os.path.join(reporting_dir, f"{municipality}_treeVis_stat")

    # Write to .parquet
    gdf_sorted.to_parquet(os.path.join(filepath + ".parquet"))

    # Write to .geojson with coord. ref. system epsg:25832
    gdf_sorted.crs = f"EPSG:{projection}"
    gdf_sorted.to_file(
        os.path.join(filepath + ".geojson"), driver="GeoJSON", encoding="utf-8"
    )

    # write to .shp with coord. ref. system epsg:25832
    gdf_sorted.to_file(os.path.join(filepath + ".shp"), driver="ESRI Shapefile")

    # Write to .csv
    gdf_sorted.to_csv(os.path.join(filepath + ".csv"), encoding="utf-8")

    return

def check_parquet_files(parquet_dict):
    
    # check value in parquet_dict
    for path in parquet_dict.values():
        # load to pd and print df head
        df = pd.read_parquet(path)
        print(f"Cols: {df.columns}")
        print(df.head())

def main(district_numbers, parquet_dir, db_path, reporting_dir):

    # add district layer to duckdb
    district_path = os.path.join(parquet_dir, f"{municipality}_districts.parquet")
    green_space_path = os.path.join(parquet_dir, f"{municipality}_green_space.parquet")
    tree_path = os.path.join(parquet_dir, f"{municipality}_tree_crowns.parquet")
    
    # load to duckdb
    load_parquet_to_duckdb(district_path, "districts", db_path)
    load_parquet_to_duckdb(green_space_path, "green_space", db_path)
    load_parquet_to_duckdb(tree_path, "tree_crowns", db_path)

    # remove per_district tables from previous run
    remove_all_except_two(db_path, "districts", "green_space", "tree_crowns")

    for district_number in district_numbers:
        print(f"Running.. district number: {district_number}")
        # get parquet files
        parquet_dict = get_parquet_dict(district_number, parquet_dir)
        
        # CHECK FILES FIRST TIME YOU RUN NEW STUDY AREA
        #check_parquet_files(parquet_dict)
        
        # load parquet files to duckdb
        for table, parquet in parquet_dict.items():
            load_parquet_to_duckdb(
                input_parquet=parquet, output_table=table, db_path=db_path
            )

        # add count columns to districts table
        add_columns(db_path, district_number)
        
        # get count statistics
        COUNT_trees = n_trees(db_path, district_number)
        COUNT_bldg = n_bldg(db_path, district_number)
        COUNT_res_bldg = n_res_bldg(db_path, district_number)
        n_green_spaces(db_path, district_number)
        COUNT_n_res_bldg_near_gs = n_res_bldg_near_gs(db_path, district_number)
        n_near_trees(db_path, district_number)
        COUNT_res_bldg_near_trees = n_res_bldg_near_trees(db_path, district_number)
        AREA_crown = a_crown(db_path, district_number)

        # update table (normalize and fill NANs)
        update_table(db_path, district_number)

        # export district to gdf
        gdf = export_districts_to_gdf(db_path, district_number)

        print(
            gdf[
                [
                    "delomradenummer",
                    "grunnkretsnummer",
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
            ]
        )

        # save count statistics to csv
        export_all_tables_to_csv(db_path, reporting_dir)

        print(f"Finished running district number: {district_number}")
        #break

    print("Finished running all districts")
    return


if __name__ == "__main__":
    
    # TODO move params to params.yml/catalog.yml
    # params
    municipality = "baerum"
    projection = 25832
    #district_numbers = range(30101, 30161) # oslo
    #district_numbers = range(180401, 180411) # bodo
    district_numbers = range(302421, 302423) # baerum

    # path to data
    root = r"/data/P-Prosjekter2/"
    root_2 = r"/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/"
    data_path = os.path.join(
        root_2, 
        "152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst", 
        "data",
        municipality
        )

    parquet_dir = os.path.join(data_path, "general","PARQUET")
    geojson_dir = os.path.join(data_path, "general","GEOJSON")
    reporting_dir = os.path.join(
        data_path, 
        "urban-tree-visibility", 
        "rule_3_30_300"
        )

    # duckdb database on P-drive
    db_path = os.path.join(parquet_dir, "duckdb_temp.db")

    # remove duckdb database
    remove_duckdb_database(db_path)

    main(district_numbers, parquet_dir, db_path, reporting_dir)

    # export all tables to csv
    concat_all_csvs(geojson_dir, reporting_dir, district_numbers, projection)
