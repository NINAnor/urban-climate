""" Module for reprojecting rasters using rasterio. """
# Adapted from:
# https://github.com/rasterio/rasterio/blob/master/examples/reproject.py

from typing import Any, Dict, Tuple

import numpy as np
from rasterio.warp import Resampling, calculate_default_transform, reproject


def reproject_raster(
    input_raster: Tuple[np.ndarray, Dict[str, Any]], dst_crs: str
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Reprojects the raster to the destination CRS.

    Args:
        raster (Tuple): Tuple containing the raw raster (numpy array) and the
        metadata (dict).
        dst_crs (str): The destination CRS.

    Returns:
        Tuple: Reprojected raster (numpy array) and metadata (dict).
    """

    # Unpack raster and metadata (image_dataset)
    array, metadata = input_raster

    # Calculate the transform for reprojection
    transform, width, height = calculate_default_transform(
        metadata["crs"],
        dst_crs,
        metadata["width"],
        metadata["height"],
        *metadata["bounds"]
    )

    # Create new dict for reprojected metadata by copying the existing one
    reprojected_metadata = metadata.copy()

    # Update the metadata for the destination CRS
    reprojected_metadata["crs"] = dst_crs
    reprojected_metadata["transform"] = transform
    reprojected_metadata["width"] = width
    reprojected_metadata["height"] = height
    # print(reprojected_metadata)

    # reproject stack of numpy arrays
    reprojected_arrays = []
    for i in range(1, metadata["count"] + 1):
        # create empty array
        reprojected_array = np.empty((height, width), dtype=array.dtype)
        # fill empty array with reprojected data
        reproject(
            source=array[i - 1],
            destination=reprojected_array,
            src_transform=metadata["transform"],
            src_crs=metadata["crs"],
            dst_transform=transform,
            dst_crs=dst_crs,
            resampling=Resampling.nearest,
        )
        reprojected_arrays.append(reprojected_array)

    print(reprojected_metadata["descriptions"])

    # repack into Tuple
    reprojected_raster = np.stack(reprojected_arrays), reprojected_metadata

    return reprojected_raster
