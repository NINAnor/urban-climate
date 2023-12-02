""" Module for resampling rasters using numpy. """
# Adapted from: https://rasterio.readthedocs.io/en/stable/topics/resampling.html
import logging
from typing import Any, Dict, Tuple

import numpy as np
from scipy.ndimage import zoom

logger = logging.getLogger(__name__)


def resample_raster(
    input_raster: Tuple[np.ndarray, Dict[str, Any]], scaling_factor: float
) -> Tuple[np.ndarray, Dict[str, Any]]:

    array, metadata = input_raster

    # Resample (nearest neighbour interpolation)
    scaling_factors = (1, scaling_factor, scaling_factor)
    resampled_array = zoom(array, scaling_factors, order=1, mode="nearest")

    # log array resampling
    logger.info("RESAMPLE ARRAY:")
    logger.info(f"Array Shape: {array.shape}")
    logger.info(f"Resampled Array Shape: {resampled_array.shape}")
    # shape = (1, 64000, 50900)

    # scale image transform
    scaled_transform = metadata["transform"] * metadata["transform"].scale(
        (metadata["width"] / resampled_array.shape[-1]),  # column
        (metadata["height"] / resampled_array.shape[-2]),  # rows
    )
    # log transform rescaling
    logger.info("RESCALE TRANSFROM:")
    logger.info(f"Transform: {metadata['transform']}")
    logger.info(f"Scaled Transform: {scaled_transform}")

    # update metadata
    new_metadata = metadata
    new_metadata["width"] = resampled_array.shape[2]  # column
    new_metadata["height"] = resampled_array.shape[1]  # rows
    new_metadata["transform"] = scaled_transform
    new_metadata["descriptions"] = "Band 1"

    resampled_raster = (resampled_array, new_metadata)

    return resampled_raster
