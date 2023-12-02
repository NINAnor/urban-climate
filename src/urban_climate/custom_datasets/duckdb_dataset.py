# create a custom Kedro dataset for DuckDB
# https://kedro.readthedocs.io/en/stable/04_user_guide/07_custom_datasets.html
# https://duckdb.org/docs/api/python

import duckdb

# import pandas as pd
from kedro.io import AbstractDataSet


class DuckDBDataSet(AbstractDataSet):
    """``DuckDBDataSet`` loads / save data from a given filepath as `pandas`
    dataframe using DuckDB.

    Example:
    ::

        >>> DuckDBDataSet(filepath='/db/file/path.db', query='SELECT * FROM data')
    """

    def __init__(self, filepath, query):
        """Creates a new instance of DuckDBDataSet to load / save data for
        given filepath.

        Args:
            filepath (_type_): _description_
            query (_type_): _description_
        """

        self.filepath = filepath
        self.query = query

    def _describe(self):
        return dict(filepath=self.filepath, query=self.query)

    def _load(self):
        """Connects to the database file and executes the query.

        Returns:
            Data from the database file as a pandas dataframe
        """

        con = duckdb.connect(self.filepath)
        return con.execute(self.query).fetchdf()

    def _save(self, data):
        con = duckdb.connect(self.filepath)
        con.execute(f"CREATE TABLE data AS {self.query}")
