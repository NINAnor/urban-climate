""" 
Add district number to building (bldg), residential buildings (res_bldg) and school property (schools) layer based on largest overlap with district layer
e.g. If building A overlaps 10m2 with district 1 and 5m2 with district 2, then building A is assigned district 1. 

District layer: districts
District number column: grunnkretsnummer
Building layer: bldg
Building id column: lokalid
Intersected building layer: int_bldg
Building area column: area_sqm
"""

#!/usr/bin/env python3

import grass.script as gs


def print_unique_values(map_layer, attribute):
    # Select distinct values of the attribute
    distinct_values = gs.read_command(
        "db.select", sql=f"SELECT DISTINCT {attribute} FROM {map_layer}"
    ).splitlines()

    # Print the number of unique values
    print(
        f"The number of unique {attribute} values in {map_layer} is {len(distinct_values)}"
    )


def print_object_count(map_layer):
    # Get topological information
    topo_info = gs.read_command("v.info", map=map_layer, flags="t")

    # Extract and print the count of objects
    object_count = int(topo_info.split("\n")[0].split("=")[1])
    print(f"The number of objects in {map_layer} is {object_count}")


def intersect(map_layers):
    gs.run_command("g.region", vector="study_area", flags="p")

    for i, map_layer in enumerate(map_layers):
        print(i, map_layer)

        # Split the map layer by the district layer
        print("split by districts")
        gs.run_command(
            "v.overlay",
            ainput=map_layer,
            binput="districts",
            operator="and",
            output="int_" + str(map_layer),
        )


def rename_cols(map_layer):
    # Get the list of columns in the layer
    columns = gs.read_command("v.info", map=map_layer, flags="c").splitlines()

    print(f"Columns before renaming: {columns}")

    # Define the list of columns that should not be renamed
    exclude_columns = ["cat", "a_cat", "a_OBJECTID", "b_cat", "b_OBJECTID"]

    # Rename all columns that start with 'a_' and are not in the exclude list
    for column in columns:
        column_name = column.split("|")[1]
        if column_name.startswith("a_") and column_name not in exclude_columns:
            new_column_name = column_name[2:]  # Remove 'a_' prefix
            gs.run_command(
                "v.db.renamecolumn",
                map=map_layer,
                column=(column_name, new_column_name),
            )
        if column_name.startswith("b_") and column_name not in exclude_columns:
            new_column_name = column_name[2:]  # Remove 'a_' prefix
            gs.run_command(
                "v.db.renamecolumn",
                map=map_layer,
                column=(column_name, new_column_name),
            )

    # Get the list of columns in the layer after renaming
    columns = gs.read_command("v.info", map=map_layer, flags="c").splitlines()
    print(f"Columns after renaming: {columns}")


def calc_area(map_layers):
    gs.run_command("g.region", vector="study_area", flags="p")

    for i, map_layer in enumerate(map_layers):
        print(i, map_layer)

        map = "int_" + str(map_layer)
        gs.run_command(
            "v.to.db",
            overwrite=True,
            map=map,
            option="area",
            columns="area_sqm",
            unit="meters",
        )


def classify_by_largest_overlap(
    original_map_layer, intersected_map_layer, col_id, col_district, col_area
):
    gs.run_command("g.region", vector="study_area", flags="p")

    # Add new column to the original map layer's attribute table
    # gs.run_command('v.db.addcolumn', map=original_map_layer, columns=f"{col_district} integer")

    # select row with max area for each district
    max_area_rows = gs.read_command(
        "db.select",
        sql=f"SELECT {col_id}, {col_district}, MAX({col_area}) FROM {intersected_map_layer} GROUP BY {col_id}",
    ).splitlines()

    # update original map layer with district number
    for row in max_area_rows:
        id, district, area = row.split("|")
        gs.run_command(
            "v.db.update",
            map=original_map_layer,
            column=col_district,
            value=district,
            where=f"{col_id} = '{id}'",
        )


def calc_attribute(map_layer, col_new, col_district):
    # add col
    gs.run_command("v.db.addcolumn", map=map_layer, columns=f"{col_new} integer")
    # calc col by stripping last two characters from col_district
    gs.run_command("v.db.update", map=map_layer, column=col_new, qcol=col_district[:-2])


if __name__ == "__main__":
    map_layers = ["bldg_copy"]
    original_map_layer = "bldg_copy"
    intersected_map_layer = "int_bldg_copy"
    col_id = "lokalid"
    col_district = "grunnkretsnummer"  # FFKKDDGG (8 numbers)
    col_area = "area_sqm"
    col_new = "delomradenummer"  # FFKKDD (6 numbers)

    # print information
    print_unique_values(original_map_layer, col_id)
    print_object_count(original_map_layer)

    # Split map layer by district layer
    intersect(map_layers)
    rename_cols(intersected_map_layer)
    print_unique_values(intersected_map_layer, f"a_{col_id}")
    print_object_count(intersected_map_layer)
    calc_area(map_layers)

    # Add district number with largest area overlap to orginal layer
    classify_by_largest_overlap(
        original_map_layer, intersected_map_layer, col_id, col_district, col_area
    )
    # Add delomrade number based on district number
    calc_attribute(original_map_layer, col_new, col_district)
