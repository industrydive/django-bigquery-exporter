import pytest
import datetime
from google.api_core.exceptions import GoogleAPICallError
from bigquery_exporter.base import batch_qs, custom_field, BigQueryExporter


class TestBatchQS:
    def test_batch_qs_empty_queryset(self, qs_factory):
        mock_qs = qs_factory(0)
        batches = list(batch_qs(mock_qs))
        assert len(batches) == 0

    def test_batch_qs_single_batch(self, qs_factory):
        mock_qs = qs_factory(5)
        batches = list(batch_qs(mock_qs, batch_size=5))
        assert len(batches) == 1
        assert batches[0] == (0, 5, 5, [1, 2, 3, 4, 5])

    def test_batch_qs_multiple_batches(self, qs_factory):
        mock_qs = qs_factory(10)
        batches = list(batch_qs(mock_qs, batch_size=3))
        assert len(batches) == 4
        assert batches[0] == (0, 3, 10, [1, 2, 3])
        assert batches[1] == (3, 6, 10, [4, 5, 6])
        assert batches[2] == (6, 9, 10, [7, 8, 9])
        assert batches[3] == (9, 10, 10, [10])

    def test_batch_qs_large_batch_size(self, qs_factory):
        mock_qs = qs_factory(5)
        batches = list(batch_qs(mock_qs, batch_size=10))
        assert len(batches) == 1
        assert batches[0] == (0, 5, 5, [1, 2, 3, 4, 5])


@pytest.fixture
def test_exporter_factory(mocker, mock_client, mock_model, qs_factory):
    def create_test_exporter(num_querysets=5, table='test_table'):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = table

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.model = mock_model
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(num_querysets)
        exporter._process_queryset = mocker.MagicMock()
        exporter._push_to_bigquery = mocker.MagicMock()
        return exporter

    return create_test_exporter


class TestBigQueryExporter:
    @pytest.fixture
    def test_exporter(self, test_exporter_factory):
        return test_exporter_factory()

    def test_export_calls_define_queryset(self, test_exporter):
        test_exporter.export()
        test_exporter.define_queryset.assert_called_once()

    def test_export_calls_process_queryset(self, test_exporter):
        test_exporter.export()
        test_exporter._process_queryset.assert_called()

    def test_export_calls_push_to_bigquery(self, test_exporter):
        test_exporter.export()
        test_exporter._push_to_bigquery.assert_called()

    def test_export_logs_error_on_google_api_call_error(self, test_exporter, caplog):
        test_exporter._push_to_bigquery.side_effect = GoogleAPICallError('Error')
        test_exporter.export()
        assert 'Error while exporting' in caplog.text

    def test_export_logs_error_on_exception(self, test_exporter, caplog):
        test_exporter._push_to_bigquery.side_effect = Exception('Error')
        test_exporter.export()
        assert 'Error while exporting' in caplog.text

    def test_custom_field_decorator_sets_custom_attribute_on_callable(self):
        @custom_field
        def test_field(self, obj):
            pass

        assert test_field.is_custom_field

    def test_custom_field_succeeds_during_processing(mocker, mock_client, mock_model):
        mock_model.field_value = 1

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = ['field_value', 'custom_field']

            @custom_field
            def custom_field(self, obj):
                return obj.field_value * 2

        # make sure we're mocking bigquery.Client
        exporter = TestBigQueryExporter()
        mock_queryset = [mock_model]
        pull_time = datetime.datetime(2023, 1, 1, 0, 0, 0)
        processed_data = exporter._process_queryset(mock_queryset, pull_time)

        assert len(processed_data) == len(mock_queryset)
        for original, processed in zip(mock_queryset, processed_data):
            assert processed['field_value'] == 1
            assert processed['custom_field'] == 2
