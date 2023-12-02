from pathlib import PurePosixPath
from typing import Any, Dict

import fsspec
import numpy as np
from kedro.io import AbstractDataset
from kedro.io.core import get_filepath_str, get_protocol_and_path
from PIL import Image


class ImageDataSet(AbstractDataset[np.ndarray, np.ndarray]):
    """``ImageDataSet`` loads / save image data from a given filepath
    as `numpy` array using Pillow.

    Example:
    ::

        >>> ImageDataSet(filepath='/img/file/path.png')
    """

    def __init__(self, filepath: str):
        """Creates a new instance of ImageDataSet to load / save image data
        for given filepath.

        Args:
            filepath: The location of the image file to load / save data.
        """
        protocol, path = get_protocol_and_path(filepath)
        self._protocol = protocol
        self._filepath = PurePosixPath(path)
        self._fs = fsspec.filesystem(self._protocol)

    def _load(self) -> np.ndarray:
        """Loads data from the image file.

        Returns:
            Data from the image file as a numpy array
        """
        load_path = get_filepath_str(self._filepath, self._protocol)
        with self._fs.open(load_path, mode="r") as f:
            image = Image.open(f).convert("RGBA")
            return np.asarray(image)

    def _save(self, data: np.ndarray) -> None:
        """Saves image data to the specified filepath."""
        save_path = get_filepath_str(self._filepath, self._protocol)
        with self._fs.open(save_path, mode="wb") as f:
            image = Image.fromarray(data)
            image.save(f)

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(filepath=self._filepath, protocol=self._protocol)
