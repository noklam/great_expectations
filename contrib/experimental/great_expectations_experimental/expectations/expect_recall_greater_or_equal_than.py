import json
from typing import Any, Dict, Tuple

from dateutil.parser import parse
from sklearn.metrics import recall_score

from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes, ExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric import MapMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.expectations.metrics.util import filter_pair_metric_nulls


#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    PandasExecutionEngine, SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration, TableExpectation,
)



# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.

class RecallGreaterOrEqualThan(MapMetricProvider):
    condition_metric_name = "column_values.recall_greater_or_equal_than"
    condition_value_keys = (
        "ignore_row_if",
        "or_equal",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
    )
    domain_keys = ("batch_id", "table", "column_A", "column_B", "threshold")

    @metric_partial(
        engine=PandasExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
        domain_type=MetricDomainTypes.COLUMN_PAIR,
    )
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        print('Random String')

        ignore_row_if = metric_value_kwargs.get("ignore_row_if")
        if not ignore_row_if:
            ignore_row_if = "both_values_are_missing"
        or_equal = metric_value_kwargs.get("or_equal")
        parse_strings_as_datetimes = metric_value_kwargs.get(
            "parse_strings_as_datetimes"
        )
        allow_cross_type_comparisons = metric_value_kwargs.get(
            "allow_cross_type_comparisons"
        )

        df, compute_domain, accessor_domain = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN_PAIR
        )

        column_A, column_B = filter_pair_metric_nulls(
            df[metric_domain_kwargs["column_A"]],
            df[metric_domain_kwargs["column_B"]],
            ignore_row_if=ignore_row_if,
        )

        threshold = metric_domain_kwargs["threshold"]

        if allow_cross_type_comparisons:
            raise NotImplementedError

        if parse_strings_as_datetimes:
            temp_column_A = column_A.map(parse)
            temp_column_B = column_B.map(parse)

        else:
            temp_column_A = column_A
            temp_column_B = column_B

        print('Test Result',  recall_score(temp_column_A, temp_column_B, average='macro'), threshold)
        if or_equal:

            return recall_score(temp_column_A, temp_column_B, average='macro') == threshold
        else:
            return recall_score(temp_column_A, temp_column_B, average='macro') >= threshold


# This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
#     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#     def _sqlalchemy(cls, column, _dialect, **kwargs):
#         return column.in_([3])

# This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
#     @column_condition_partial(engine=SparkDFExecutionEngine)
#     def _spark(cls, column, **kwargs):
#         return column.isin([3])


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectRecallGreaterOrEqualThan(TableExpectation):
    """
    This expectation tests if recall is larger or equal than a certain threshold. Recall is a common metric that used
    in binary classification task.
    """

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.recall_greater_or_equal_than"

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a": [0, 1, 2, 0, 1, 2],
                "b": [0, 2, 1, 0, 0, 1],
            },
            "schemas": {},
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "in": {"column_A": "a", "column_B": "b", "threshold": 0.3},
                    "out": {"success": True},
                    "include_in_gallery": True,
                    "only_for": [
                        "pandas"
                    ]
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "in": {"column_A": "a", "column_B": "b", "threshold": 0.1},
                    "out": {"success": False},
                    "include_in_gallery": True,
                    "only_for": [
                        "pandas"
                    ]
                },

            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "experimental",
          ],
        "contributors": ["@noklam"
        ],
        "package": "experimental_expectations",
    }



    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = (
        "column_A",
        "column_B",
        "threshold",
        "ignore_row_if",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return {
            "success": True,
            "result": {
                "details": {
                    "dickens_say": "Contributing to open source makes the world a better place."
                }
            },
        }


    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
    #!!! This example renderer should render RenderedStringTemplateContent, not just a string


#     @classmethod
#     @renderer(renderer_type="renderer.question")
#     def _question_renderer(
#         cls, configuration, result=None, language=None, runtime_configuration=None
#     ):
#         column = configuration.kwargs.get("column")
#         mostly = configuration.kwargs.get("mostly")

#         return f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'

# This method defines an answer Renderer
#!!! This example renderer should render RenderedStringTemplateContent, not just a string
#     @classmethod
#     @renderer(renderer_type="renderer.answer")
#     def _answer_renderer(
#         cls, configuration=None, result=None, language=None, runtime_configuration=None
#     ):
#         column = result.expectation_config.kwargs.get("column")
#         mostly = result.expectation_config.kwargs.get("mostly")
#         regex = result.expectation_config.kwargs.get("regex")
#         if result.success:
#             return f'At least {mostly * 100}% of values in column "{column}" equal 3.'
#         else:
#             return f'Less than {mostly * 100}% of values in column "{column}" equal 3.'

# This method defines a prescriptive Renderer
#     @classmethod
#     @renderer(renderer_type="renderer.prescriptive")
#     @render_evaluation_parameter_string
#     def _prescriptive_renderer(
#         cls,
#         configuration=None,
#         result=None,
#         language=None,
#         runtime_configuration=None,
#         **kwargs,
#     ):
#!!! This example renderer should be shorter
#         runtime_configuration = runtime_configuration or {}
#         include_column_name = runtime_configuration.get("include_column_name", True)
#         include_column_name = (
#             include_column_name if include_column_name is not None else True
#         )
#         styling = runtime_configuration.get("styling")
#         params = substitute_none_for_missing(
#             configuration.kwargs,
#             ["column", "regex", "mostly", "row_condition", "condition_parser"],
#         )

#         template_str = "values must be equal to 3"
#         if params["mostly"] is not None:
#             params["mostly_pct"] = num_to_str(
#                 params["mostly"] * 100, precision=15, no_scientific=True
#             )
#             # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
#             template_str += ", at least $mostly_pct % of the time."
#         else:
#             template_str += "."

#         if include_column_name:
#             template_str = "$column " + template_str

#         if params["row_condition"] is not None:
#             (
#                 conditional_template_str,
#                 conditional_params,
#             ) = parse_row_condition_string_pandas_engine(params["row_condition"])
#             template_str = conditional_template_str + ", then " + template_str
#             params.update(conditional_params)

#         return [
#             RenderedStringTemplateContent(
#                 **{
#                     "content_block_type": "string_template",
#                     "string_template": {
#                         "template": template_str,
#                         "params": params,
#                         "styling": styling,
#                     },
#                 }
#             )
#         ]

if __name__ == "__main__":
    diagnostics_report = (
        ExpectRecallGreaterOrEqualThan().run_diagnostics()
    )
    print(json.dumps(diagnostics_report, indent=2))
