import os
import arcpy

def main(municipality, geojson_dir, layer_dict, table_names):
    # {table: path}
    geojson_dict = {
        tbl: os.path.join(geojson_dir, f"{municipality}_{tbl}.geojson")
        for tbl in table_names
    }

    # print the dictionaries
    print(layer_dict)
    print(geojson_dict)

    for lyr, geojson in zip(layer_dict.values(), geojson_dict.values()):
        print(lyr)
        print(geojson)
        arcpy.conversion.FeaturesToJSON(
            in_features=lyr,
            out_json_file=geojson,
            format_json="NOT_FORMATTED",
            include_z_values="NO_Z_VALUES",
            include_m_values="NO_M_VALUES",
            geoJSON="GEOJSON",
            outputToWGS84="KEEP_INPUT_SR",
            use_field_alias="USE_FIELD_NAME",
        )

    return print(f"Converted to geojson: {table_names}")


if __name__ == "__main__":
    root = r"P:\152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst\data"
    municipality = "bodo"
    geojson_dir = os.path.join(root, municipality, "general", "GEOJSON")

    # FILEGDBS
    gdb_dir_trees = os.path.join(
        root, municipality, "general", "bytraer_database", municipality + "_bytraer.gdb"
    )
    gdb_dir_admin = os.path.join(
        root, municipality, "general", "FILEGDB", municipality + "_admindata.gdb"
    )
    gdb_dir_basis = os.path.join(
        root, municipality, "general", "FILEGDB", municipality + "_basisdata.gdb"
    )
    gdb_dir_areal = os.path.join(
        root, municipality, "general", "FILEGDB", municipality + "_arealdata.gdb"
    )

    # {layer: path}
    layer_dict = {
        "analyseomrade_no_sea": os.path.join(gdb_dir_admin, "analyseomrade_no_sea"),
        "grunnkretser": os.path.join(gdb_dir_admin, "grunnkretser"),
        "fkb_bygning_omrade": os.path.join(gdb_dir_basis, "fkb_bygning_omrade"),
        "fkb_boligbygg_omrade": os.path.join(gdb_dir_areal, "fkb_boligbygg_omrade"),
        "ssb_grontomrade": os.path.join(gdb_dir_areal, "ssb_grontomrade"),
        "apent_omrade": os.path.join(gdb_dir_areal, "apent_omrade"),
        "offentlig_apent_omrade": os.path.join(gdb_dir_areal, "offentlig_apent_omrade"),
        "privat_apent_omrade": os.path.join(gdb_dir_areal, "privat_apent_omrade"),
        "trekroner_2017": os.path.join(gdb_dir_trees, "trekroner_2017"),
        "registrerte_traer_fullstendig": os.path.join(
            gdb_dir_trees, "registrerte_traer_fullstendig"
        ),
    }

    table_names = [
        # "study_area",
        # "districts",
        # "bldg",
        # "res_bldg",
        # "green_space",
        # "open_space",
        # "public_open_space",
        # "private_open_space",
        # "tree_crowns",
        # "registered_trees",
    ]

    main(municipality, geojson_dir, layer_dict, table_names)
