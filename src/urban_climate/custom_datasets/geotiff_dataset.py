import logging
import os
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Tuple

import fsspec

# import geopandas as gpd
import numpy as np
import rasterio
import rasterio.mask
from kedro.io import AbstractDataset
from kedro.io.core import get_filepath_str, get_protocol_and_path

logger = logging.getLogger(__name__)
# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent


class GeoTIFFDataSet(AbstractDataset[np.ndarray, np.ndarray]):
    """``ImageDataSet`` loads / save image data from a given filepath as
    `numpy` array using Rasterio.

    Example:
    ::

        >>> ImageDataSet(filepath='/img/file/path.tif')
    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {"mask": False}

    def __init__(self, filepath: str, load_args: Dict[str, Any] = None) -> None:
        """Creates a new instance of ImageDataSet to load / save image data
        for given filepath.

        Args:
            filepath: The location of the image file to load / save data.
        """
        protocol, path = get_protocol_and_path(filepath)
        self._protocol = protocol
        self._filepath = PurePosixPath(path)
        self._fs = fsspec.filesystem(self._protocol)
        self.test = None

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)

    def exists(self) -> bool:
        """Checks if the data at the given filepath exists.

        Returns:
            True if data exists, else False.
        """
        load_path = get_filepath_str(self._filepath, self._protocol)
        return self._fs.exists(load_path)

    def _load(self) -> Tuple[np.ndarray, Any, Any]:
        """Loads data from the image file.

        Returns:
            Data from the image file as a numpy array
        """
        load_path = get_filepath_str(self._filepath, self._protocol)
        with self._fs.open(load_path, mode="rb") as f:
            load_args = self._load_args
            logger.info(load_args)

            if load_args["mask"]:
                logger.info("Loading data with mask...")
                logger.info(project_root)
                import fiona

                path = os.path.join(project_root, load_args["mask_path"])
                logger.info(path)

                with fiona.open(path, "r") as shp:
                    shapes = [feature["geometry"] for feature in shp]
                logger.info(f"Shapes: {shapes}")

                with rasterio.open(f) as src:
                    data, out_transform = rasterio.mask.mask(src, shapes, crop=True)
                    out_meta = src.meta.copy()

                    out_meta.update(
                        {
                            "driver": "GTiff",
                            "height": data.shape[1],
                            "width": data.shape[2],
                            "transform": out_transform,
                        }
                    )

                    # add additional metadata
                    out_meta["bounds"] = src.bounds
                    out_meta["descriptions"] = src.descriptions
                    out_meta["creation_time"] = src.profile.get("creation_time")

                    # from urban_climate_services.utils.raster.mask import mask_raster
                    # masked_raster = mask_raster(data, mask_gdf)

            else:
                logger.info("Loading data without mask...")
                with rasterio.open(f) as src:
                    # load data
                    data = src.read()

                    # copy metadata to dict
                    out_meta = src.meta.copy()

                    # add additional metadata
                    out_meta["bounds"] = src.bounds
                    out_meta["descriptions"] = src.descriptions
                    out_meta["creation_time"] = src.profile.get("creation_time")

            logger.info(f"Number of bands: {out_meta['count']}")
            logger.info(f"Band names: {out_meta['descriptions']}")
            logger.info(f"CRS: {out_meta['crs']}")
            logger.info(f"time: {out_meta['creation_time']}")
            logger.info(f"Geotransform: {out_meta['transform']}")
            logger.info(f"Bounds: {src.bounds}")

            return data, out_meta

    def _save(self, data: Tuple) -> None:
        """Saves GeoTIFF data to the specified filepath."""
        import time

        save_path = get_filepath_str(self._filepath, self._protocol)
        array, metadata = data

        logger.info(f"Band names: {metadata['descriptions']}")
        logger.info(f"CRS: {metadata['crs']}")
        logger.info(f"Geotransform: {metadata['transform']}")

        # unpack additional metadata
        bounds = metadata["bounds"]
        descriptions = metadata["descriptions"]
        creation_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        logger.info(f"Bounds: {bounds}")
        logger.info("Creation time: {}".format(creation_time))

        # remove additional metadata from dict
        metadata.pop("bounds", None)
        metadata.pop("descriptions", None)
        metadata.pop("creation_time", None)

        # save numpy array as GeoTIFF file
        with self._fs.open(save_path, mode="wb") as f:
            with rasterio.open(
                f,
                "w",
                **metadata,  # unpack metadata dict
            ) as dst:
                # Set band descriptions
                if dst.count == len(descriptions):
                    dst.descriptions = descriptions
                else:  # pragma: no cover
                    logger.warning(
                        "Number of band descriptions does not match number of bands.\
                            Band descriptions will not be set."
                    )
                dst.profile["creation_time"] = creation_time
                dst.write(array)

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(filepath=self._filepath, protocol=self._protocol)
