import pytest
from google.api_core.exceptions import GoogleAPICallError
from bigquery_exporter.base import batch_qs, BigQueryExporter


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


class TestBigQueryExporter:
    def test_export_calls_define_queryset(self, mocker, mock_client, mock_model):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test'

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.define_queryset = mocker.MagicMock()
        exporter.export()
        exporter.define_queryset.assert_called_once()

    def test_export_calls_process_queryset(self, mocker, mock_client, mock_model, qs_factory):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test'

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.model = mock_model
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(5)
        exporter._process_queryset = mocker.MagicMock()
        exporter.export()
        exporter._process_queryset.assert_called()

    def test_export_calls_push_to_bigquery(self, mocker, mock_client, mock_model, qs_factory):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test'

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.model = mock_model
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(5)
        exporter._push_to_bigquery = mocker.MagicMock()
        exporter.export()
        exporter._push_to_bigquery.assert_called()

    def test_export_logs_error_on_google_api_call_error(self, mocker, mock_client, mock_model, caplog, qs_factory):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test'

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.model = mock_model
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(5)
        exporter._push_to_bigquery = mocker.MagicMock()
        exporter._push_to_bigquery.side_effect = GoogleAPICallError('Error')
        exporter.export()
        assert 'Error while exporting' in caplog.text

    def test_export_logs_error_on_exception(self, mocker, mock_client, mock_model, caplog, qs_factory):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test'

        exporter = TestExporter()
        exporter.client = mock_client
        exporter.model = mock_model
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(5)
        exporter._process_queryset = mocker.MagicMock()
        exporter._process_queryset.side_effect = Exception('Error')
        exporter.export()
        assert 'Error while exporting' in caplog.text