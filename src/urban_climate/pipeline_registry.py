"""Project pipelines."""
from __future__ import annotations

from typing import Dict

# from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

# import pipelines here
from .pipelines import data_science, raster_processing


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # pipelines = find_pipelines()

    pipelines = {
        "raster_processing": raster_processing.create_pipeline(),
        "data_science": data_science.create_pipeline(),
    }

    pipelines["__default__"] = sum(pipelines.values())
    return pipelines
