from kedro.pipeline import Pipeline, node

from .nodes import mask_raster, reproject_raster, stack_rasters, stack_to_gdf

# TODO store reprojected rasters in memory and pass them to stack_rasters node
# TODO move stack_rasters to a util function
# TODO update custom load to include mask (finished but not very elegant)
# TODO load crowns as vectors and convert to raster (now performed in GIS)


def create_pipeline(**kwargs) -> Pipeline:
    name_list = ["terrain", "landcover_fraction", "land_surface_temperature"]
    mask_list = ["masked_" + raster for raster in name_list]
    reprojected_rasters = ["r_" + raster for raster in mask_list]

    # Node: mask_rasters (loop over raster_list)
    mask_nodes = []
    for raster in name_list:
        mask_node = node(
            mask_raster,
            inputs=[raster],
            outputs=f"masked_{raster}",
            name=f"mask_{raster}",
            tags=["mask_raster"],
        )
        mask_nodes.append(mask_node)

    # Node: reproject_raster (loop over raster_list)
    reproject_nodes = []
    for raster, name in zip(mask_list, name_list):
        reproject_node = node(
            reproject_raster,
            inputs=[raster, "params:dst_crs"],
            outputs=f"r_{raster}",
            name=f"reproject_{name}",
            tags=["reproject_raster"],
        )
        reproject_nodes.append(reproject_node)

    # Node: raster stack
    stack_node = node(
        stack_rasters,
        inputs=[f"{name}" for name in reprojected_rasters] + ["canopy_fraction"],
        outputs="raster_stack",
        name="raster_stack",
        tags=["raster_stack"],
    )

    # Node: stack to gdf
    stack_to_gdf_node = node(
        stack_to_gdf,
        inputs=["raster_stack", "study_area", "params:path_to_stack"],
        outputs="gdf_model_input",
        name="stack_to_gdf",
        tags=["stack_to_gdf"],
    )
    # Define the pipeline by combining reproject and stack nodes
    pipeline = Pipeline(mask_nodes + reproject_nodes + [stack_node, stack_to_gdf_node])

    return pipeline
