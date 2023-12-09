""" 
Split green, open, public and private space layers by district layer.
and add the district (grunnkretsnummer and delomradenummer) number to the attribute table.
"""


#!/usr/bin/env python3

import grass.script as gs


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


def drop_cols(map_layer):
    # Get the list of columns in the layer
    columns = gs.read_command("v.info", map=map_layer, flags="c").splitlines()

    print(f"Columns before cleaning: {columns}")

    # Define the list of mandatory columns that should not be dropped
    mandatory_columns = [
        "cat",
        "a_cat",
        "a_OBJECTID",
        "b_cat",
        "b_OBJECTID",
        "b_SHAPE_Length",
        "b_SHAPE_Area",
        "b_grunnkretsnummer",
        "b_delomradenummer",
    ]

    # Drop all columns except for the mandatory ones
    for column in columns:
        column_name = column.split("|")[1]
        if not column_name.startswith("a_") and column_name not in mandatory_columns:
            # if column_name not in mandatory_columns:
            gs.run_command("v.db.dropcolumn", map=map_layer, column=column_name)

    # Get the list of columns in the layer after cleaning
    columns = gs.read_command("v.info", map=map_layer, flags="c").splitlines()
    print(f"Columns after cleaning: {columns}")


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


def clean_cols(map_layers):
    gs.run_command("g.region", vector="study_area", flags="p")

    for i, map_layer in enumerate(map_layers):
        map = "int_" + str(map_layer)
        drop_cols(map)
        rename_cols(map)


if __name__ == "__main__":
    map_layers = ["green_space"]
    intersect(map_layers)
    clean_cols(map_layers)
