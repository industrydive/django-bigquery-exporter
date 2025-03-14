import pytest
import datetime
from google.api_core.exceptions import GoogleAPICallError, RetryError

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

    def test_batch_qs_with_batch_size_none(self, qs_factory):
        """
        Verify that when batch_size is None, one batch is returned
        that contains the entire queryset.
        """
        mock_qs = qs_factory(10)
        batches = list(batch_qs(mock_qs, batch_size=None))
        assert len(batches) == 1
        # Assert start, end, total, and that queryset is the original mock_qs
        assert batches[0] == (0, 10, 10, mock_qs)
        # Verify the queryset contents
        assert list(batches[0][3]) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@pytest.fixture
def test_exporter_factory(mocker, mock_client, mock_model, qs_factory):
    def create_test_exporter(qs_size=5, table='test_table', batch_size=1000, qs_ordered=True):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = table
            batch = batch_size

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.model = mock_model
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(qs_size, qs_ordered)
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

    def test_export_does_not_call_define_queryset_when_queryset_provided(self, test_exporter, qs_factory):
        test_exporter.export(queryset=qs_factory(5))
        test_exporter.define_queryset.assert_not_called()

    def test_export_calls_process_queryset(self, test_exporter):
        test_exporter.export()
        test_exporter._process_queryset.assert_called()

    def test_export_calls_push_to_bigquery(self, test_exporter):
        test_exporter.export()
        test_exporter._push_to_bigquery.assert_called()

    def test_export_logs_error_on_google_api_call_error(self, test_exporter, caplog):
        with pytest.raises(GoogleAPICallError):
            test_exporter._push_to_bigquery.side_effect = GoogleAPICallError('Error', 'error')
            test_exporter.export()
            assert 'Error pushing TestExporter' in caplog.text
            assert 'GoogleAPICallError' in caplog.text

    def test_export_logs_error_on_exception(self, test_exporter, caplog):
        with pytest.raises(RetryError):
            test_exporter._push_to_bigquery.side_effect = RetryError('Error', 'error')
            test_exporter.export()
            assert 'Error pushing TestExporter' in caplog.text
            assert 'RetryError' in caplog.text

    def test_custom_field_decorator_sets_custom_attribute_on_callable(self):
        @custom_field
        def test_field(self, obj):
            pass

        assert test_field.is_custom_field

    def test_custom_field_succeeds_during_processing(self, bigquery_client_factory, mocker, mock_model):
        mock_model.field_value = 1
        # mock the client to return a table with the field 'field_value' and 'custom_field'
        mock_client = bigquery_client_factory('test_table', ['field_value', 'custom_field'])
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

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

    def test_export_raises_type_error_for_invalid_pull_date_type(self, test_exporter):
        with pytest.raises(TypeError) as exc_info:
            # Passing a string instead of datetime
            test_exporter.export(pull_date='2025-01-01')
        assert 'Expected a datetime.datetime object for pull_date, but got str instead' in str(exc_info.value)

    def test_export_raises_type_error_for_invalid_queryset_type(self, test_exporter, mock_model):
        with pytest.raises(TypeError) as exc_info:
            # Passing a list containing a model instance instead of QuerySet
            test_exporter.export(queryset=[mock_model()])
        assert 'Expected a Django QuerySet, but got list instead' in str(exc_info.value)

    def test_export_raises_value_error_when_unordered_queryset_larger_than_batch(self, test_exporter_factory):
        test_exporter = test_exporter_factory(qs_size=5, batch_size=3, qs_ordered=False)
        with pytest.raises(ValueError) as exc_info:
            test_exporter.export()
        assert 'Queryset must be ordered (using .order_by()) when batch size (3) is smaller than queryset size (5)' in str(exc_info.value)
