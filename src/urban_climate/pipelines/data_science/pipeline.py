# modular pipeline
# https://docs.kedro.org/en/stable/tutorial/add_another_pipeline.html
# default, runs nodes one after another
# kedro run --runner=SequentialRunner
# multiprocessing takes advantage of multiple cores on a single machine
# kedro run --runner=ParallelRunner
# mulithreading inteded for remote execution such as Spark, Dask, etc.
# kedro run --runner=ThreadRunner
# custom
# kedro run --runner=module.path.to.my.runner

from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import evaluate_model, predict_counterfactual, split_data, train_model


def create_pipeline(**kwargs) -> Pipeline:
    pipeline_instance = pipeline(
        [
            node(
                func=split_data,
                inputs=["gdf_model_input", "params:model_options"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_node",
            ),
            node(
                func=train_model,
                inputs=["X_train", "y_train"],
                outputs="regressor",
                name="train_model_node",
            ),
            node(
                func=evaluate_model,
                inputs=["regressor", "X_test", "y_test"],
                outputs=None,
                name="evaluate_model_node",
            ),
            node(
                func=predict_counterfactual,
                inputs=["gdf_model_input", "regressor", "params:model_options"],
                outputs="gdf_counterfactual",
                name="predict_counterfactual_node",
            ),
        ]
    )
    ds_pipeline_1 = pipeline(
        pipe=pipeline_instance,
        inputs="gdf_model_input",
        namespace="active_modelling_pipeline",
    )
    ds_pipeline_2 = pipeline(
        pipe=pipeline_instance,
        inputs="gdf_model_input",
        namespace="candidate_modelling_pipeline",
    )

    return ds_pipeline_1 + ds_pipeline_2


# non-modular pipeline
# from kedro.pipeline import Pipeline, node, pipeline

# from .nodes import evaluate_model, split_data, train_model


# def create_pipeline(**kwargs) -> Pipeline:
#     return pipeline(
#         [
#             node(
#                 func=split_data,
#                 inputs=["model_input_table", "params:model_options"],
#                 outputs=["X_train", "X_test", "y_train", "y_test"],
#                 name="split_data_node",
#             ),
#             node(
#                 func=train_model,
#                 inputs=["X_train", "y_train"],
#                 outputs="regressor",
#                 name="train_model_node",
#             ),
#             node(
#                 func=evaluate_model,
#                 inputs=["regressor", "X_test", "y_test"],
#                 outputs=None,
#                 name="evaluate_model_node",
#             ),
#         ]
#     )
