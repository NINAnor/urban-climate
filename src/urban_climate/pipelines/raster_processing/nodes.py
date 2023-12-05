import logging

# from rasterio.warp import calculate_default_transform, reproject, Resampling
from typing import Any, Dict, Tuple

import geopandas as gpd
import numpy as np

# import rasterio
# from rasterio.features import geometry_mask
# from rasterio.transform import Affine
# from scipy.ndimage import zoom

logger = logging.getLogger(__name__)


def mask_raster(
    input_raster: Tuple[np.ndarray, Dict[str, Any]]
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Saves the mask to a file.

    Args:
        mask: Mask to save.
        filepath: Filepath to save to.
        save_args: Save arguments.
    """

    data, meta = input_raster

    return data, meta


def reproject_raster(
    input_raster: Tuple[np.ndarray, Dict[str, Any]], dst_crs: str
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Preprocesses the elevation data.

    Args:
        elevation: Raw data.

    Returns:
        Preprocessed data, with numpy array reprojected to EPSG:25832 and
        metadata updated.
    """

    from urban_climate.utils.raster.reproject import reproject_raster

    output_raster = reproject_raster(input_raster, dst_crs=dst_crs)
    return output_raster


def stack_rasters(
    reprojected_terrain,
    reprojected_landcover_fraction,
    reprojected_land_surface_temperature,
    canopy_fraction,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Stacks reprojected rasters, and updates metadata.

    Args:
        reprojected_terrain (tuple): numpy array, metadata
        reprojected_landcover_fraction tuple): numpy array, metadata
        reprojected_land_surface_temperature (tuple): numpy array, metadata

    Raises:
        ValueError: check that CRS is the same for all rasters
        ValueError: check that shape of axis1, axis2 is the same for all
        rasters

    Returns:
        Tuple[np.ndarray, Dict[str, Any]]: concatenated numpy array, metadata
    """

    raster_list = [
        reprojected_terrain,
        reprojected_landcover_fraction,
        reprojected_land_surface_temperature,
        canopy_fraction,
    ]

    # unpack to arrays and metadata
    reprojected_array = [raster[0] for raster in raster_list]
    metadata = [raster[1] for raster in raster_list]

    # for array, meta in zip(reprojected_array, metadata):
    #      logger.info(f"Band Name: {meta['descriptions']} \t CRS: {meta['crs']}\
    #          \tArray shape: {array.shape}")

    # check CRS
    crs = metadata[0].get("crs", None)
    if not all(meta.get("crs", None) == crs for meta in metadata):
        raise ValueError("Input rasters must have the same CRS.")
    else:
        logger.info(f"CRS: {crs}")

    # Array Shape (1, 640 509) -> (axis0, axis1, axis2)
    # check axis 1 and axis 2
    n_array = len(reprojected_array)
    if not all(
        reprojected_array[i][0].shape == reprojected_array[0][0].shape
        for i in range(n_array)
    ):
        # print shape of each array
        for i in range(n_array):
            logger.info(f"Array shape: {reprojected_array[i][0].shape}")
        raise ValueError("Input rasters must have the same shape.")
    else:
        logger.info(f"Array shape: {reprojected_array[0][0].shape}")

    # CONCAT NUMPY ARRAYS ALONG AXIS0
    np_concat = np.concatenate(reprojected_array, axis=0)
    logger.info(f"Concatenated Array Shape: {np_concat.shape}")

    # CREATE METADATA FOR NUMPY STACK
    # extract band descriptions
    stack_descriptions = [
        desc for meta in metadata for desc in meta.get("descriptions", [])
    ]
    stack_descriptions = [
        "DTM",
        "fractionBuilt",
        "fractionCropland",
        "fractionGrass",
        "fractionWater",
        "LST",
        "fractionCanopy",
    ]
    logger.info(f"Stack Descriptions: {stack_descriptions}")

    # copy metadata from first file
    stack_metadata = metadata[0]
    stack_metadata["descriptions"] = stack_descriptions
    stack_metadata["count"] = len(np_concat)
    logger.info(f"Copy Metadata: {stack_metadata}")

    raster_stack = np_concat, stack_metadata

    return raster_stack


def stack_to_gdf(raster_stack, study_area, path_copy_stack):
    """Converts a raster stack to a GeoDataFrame.
    Each band becomes a column, and each pixel value is a row.
    Args:
        path (str): The file path to the raster stack.
    Returns:
        gdf (geopandas.GeoDataFrame): The GeoDataFrame.
    """

    # meta = raster_stack[1]
    path = path_copy_stack

    import pandas as pd
    import rasterio as rio

    with rio.Env():
        with rio.open(path) as src:
            crs = src.crs

            # create 1D coordinate arrays (coordinates of the pixel center)
            xmin, ymax = np.around(src.xy(0.00, 0.00), 9)  # src.xy(0, 0)
            xmax, ymin = np.around(
                src.xy(src.height - 1, src.width - 1), 9
            )  # src.xy(src.width-1, src.height-1)
            x = np.linspace(xmin, xmax, src.width)
            y = np.linspace(
                ymax, ymin, src.height
            )  # max -> min so coords are top -> bottom

            # create 2D arrays
            xs, ys = np.meshgrid(x, y)

            # read all bands and their descriptions
            b1 = src.read(1)
            b2 = src.read(2)
            b3 = src.read(3)
            b4 = src.read(4)
            b5 = src.read(5)
            b6 = src.read(6)
            b7 = src.read(7)

            b1_name = src.descriptions[0]

            # update band names to [b1_name, b2_name, ...]
            # logger.info(src.descriptions)
            # src.descriptions = [f"b{i}" for i in range(1, 8)]
            # logger.info(f"Band Names: {src.descriptions}")

            logger.info(f"Band Name: {b1_name}")

            # Apply NoData mask
            mask = src.read_masks(1) > 0
            xs, ys, b1, b2, b3, b4, b5, b6, b7 = (
                xs[mask],
                ys[mask],
                b1[mask],
                b2[mask],
                b3[mask],
                b4[mask],
                b5[mask],
                b6[mask],
                b7[mask],
            )

    data = {
        "X": pd.Series(xs.ravel()),
        "Y": pd.Series(ys.ravel()),
        src.descriptions[0]: pd.Series(b1.ravel()),
        src.descriptions[1]: pd.Series(b2.ravel()),
        src.descriptions[2]: pd.Series(b3.ravel()),
        src.descriptions[3]: pd.Series(b4.ravel()),
        src.descriptions[4]: pd.Series(b5.ravel()),
        src.descriptions[5]: pd.Series(b6.ravel()),
        "fractionCanopy": pd.Series(b7.ravel()),
    }

    df = pd.DataFrame(data=data)
    geometry = gpd.points_from_xy(df.X, df.Y)
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    # clip to study area
    gdf_clip = gpd.clip(gdf, study_area)

    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    gdf.plot(ax=ax1, color="blue")
    gdf_clip.plot(ax=ax2, color="purple")
    ax1.set_title("All Unclipped pixels", fontsize=20)
    ax2.set_title("Clipped pixels", fontsize=20)
    ax1.set_axis_off()
    ax2.set_axis_off()
    plt.show()

    logger.info(gdf.head())
    return gdf_clip
