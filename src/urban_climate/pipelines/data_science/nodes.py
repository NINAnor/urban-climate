import logging
from typing import Dict, Tuple

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split


def split_data(data, parameters: Dict) -> Tuple:
    """Splits data into features and targets training and test sets.

    Args:
        data: Data containing features and target.
        parameters: Parameters defined in parameters/data_science.yml.
    Returns:
        Split data.
    """

    # drop rows with missing values
    data = data.dropna()

    X = data[parameters["features"]]
    y = data["LST"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=parameters["test_size"], random_state=parameters["random_state"]
    )
    return X_train, X_test, y_train, y_test


def train_model(X_train: pd.DataFrame, y_train: pd.Series) -> LinearRegression:
    """Trains the linear regression model.

    Args:
        X_train: Training data of independent features.
        y_train: Training data for price.

    Returns:
        Trained model.
    """
    regressor = LinearRegression()
    regressor.fit(X_train, y_train)
    return regressor


def evaluate_model(
    regressor: LinearRegression, X_test: pd.DataFrame, y_test: pd.Series
):
    """Calculates and logs the coefficient of determination.

    Args:
        regressor: Trained model.
        X_test: Testing data of independent features.
        y_test: Testing data for price.
    """
    y_pred = regressor.predict(X_test)
    score = r2_score(y_test, y_pred)
    logger = logging.getLogger(__name__)
    logger.info("Model has a coefficient R^2 of %.3f on test data.", score)


# Create a copy of the dataframe with LST column removed and
# canopyFraction set to 0 or nodata if nodata
# TODO add node to predict LST from model


def predict_counterfactual(
    data, regressor: LinearRegression, parameters: Dict
) -> pd.DataFrame:
    """Predicts the counterfactual LST values for the input data.

    Args:
        data: Data containing features and target.
        regressor: Trained model.

    Returns:
        Dataframe with counterfactual LST values.
    """

    # interpolate missing values BUT NOT IN ROW GEOMETRY!
    data["DTM"] = data["DTM"].interpolate(distance=2)
    data["fractionGrass"] = data["fractionGrass"].interpolate(distance=2)
    data["fractionCropland"] = data["fractionCropland"].interpolate(distance=2)
    data["fractionBuilt"] = data["fractionBuilt"].interpolate(distance=2)
    data["fractionWater"] = data["fractionWater"].interpolate(distance=2)

    # set canopyFraction to 0
    data["fractionCanopy"] = 0

    # remove column LST and canopyFraction
    data = data.drop(columns=["LST"])

    # drop rows with missing values
    data = data.dropna()

    # predict LST
    X = data[parameters["features"]]
    y_pred = regressor.predict(X)

    # add predicted LST to dataframe
    # created new columen predicted LST
    data["predicted_LST"] = y_pred

    return data
