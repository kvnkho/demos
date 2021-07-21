import datetime
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier

context = ge.data_context.DataContext()

# Feel free to change the name of your suite here. Renaming this will not
# remove the other one.
expectation_suite_name = "taxi.demo"
suite = context.get_expectation_suite(expectation_suite_name)
suite.expectations = []

batch_kwargs = {'path': '/Users/kevinkho/Work/demos/conferences/dremio-subsurface/great_expectations/.././test-file.csv', 'datasource': '.__dir', 'data_asset_name': 'test-file'}
batch = context.get_batch(batch_kwargs, suite)

batch.save_expectation_suite(discard_failed_expectations=False)

results = LegacyCheckpoint(
    name="_temp_checkpoint",
    data_context=context,
    batches=[
        {
          "batch_kwargs": batch_kwargs,
          "expectation_suite_names": [expectation_suite_name]
        }
    ]
).run()

run_info_at_end = True
validation_results_page_renderer = (
    ge.render.renderer.ValidationResultsPageRenderer(
        run_info_at_end=run_info_at_end
    )
)
rendered_document_content_list = (
    validation_results_page_renderer.render_validation_operator_result(
        validation_operator_result=results
    )
)
markdown_artifact = " ".join(
    ge.render.view.DefaultMarkdownPageView().render(
        rendered_document_content_list
    )
)