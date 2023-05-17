import pytest
from unittest.mock import Mock, patch
from bigquery_exporter.base import batch_qs, BigQueryExporter
from google.api_core.exceptions import GoogleAPICallError


def test_batch_qs_empty_queryset():
    queryset = Mock()
    queryset.count.return_value = 0
    batches = list(batch_qs(queryset))
    assert batches == []


def test_batch_qs_smaller_than_batch_size():
    queryset = Mock()
    queryset.count.return_value = 5
    batches = list(batch_qs(queryset, batch_size=10))
    assert len(batches) == 1


def test_batch_qs_larger_than_batch_size():
    queryset = Mock()
    queryset.count.return_value = 25
    batches = list(batch_qs(queryset, batch_size=10))
    assert len(batches) == 3


@pytest.fixture
def setup_bigquery_exporter():
    mock_client = Mock()

    with patch('bigquery_exporter.base.bigquery.Client') as mock_client_class, patch('bigquery_exporter.base.logging') as mock_logging:
        mock_client_class.return_value = mock_client

        yield mock_client, mock_logging


def test_init_with_no_model():
    with pytest.raises(AssertionError):
        BigQueryExporter()


def test_init_with_no_table_name():
    with pytest.raises(AssertionError):
        class TestExporter(BigQueryExporter):
            model = Mock()
        TestExporter()


def test_init_with_invalid_custom_fields():
    with pytest.raises(ValueError):
        class TestExporter(BigQueryExporter):
            model = Mock()
            table_name = 'test_table'
            custom_fields = ['not_a_method']
        TestExporter()


def test_export_with_no_errors(setup_bigquery_exporter):
    mock_client, mock_logging = setup_bigquery_exporter

    class TestExporter(BigQueryExporter):
        model = Mock()
        table_name = 'test_table'
    exporter = TestExporter()
    exporter.export()
    mock_client.insert_rows.assert_called()


def test_export_with_bigquery_errors(setup_bigquery_exporter):
    mock_client, mock_logging = setup_bigquery_exporter

    class TestExporter(BigQueryExporter):
        model = Mock()
        table_name = 'test_table'
    exporter = TestExporter()
    mock_client.insert_rows.side_effect = GoogleAPICallError('Test error')
    exporter.export()
    mock_logging.error.assert_called_with('Error while exporting Mock: Test error')
