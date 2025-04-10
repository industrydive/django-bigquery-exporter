import pytest

from django.db import models
from google.cloud import bigquery

from bigquery_exporter.base import BigQueryExporter
from bigquery_exporter.clients.interface import BigQueryClientInterface

@pytest.fixture
def mock_field_factory(mocker):
    def create_mock_field(name, field_type):
        field = mocker.MagicMock()
        field.name = name
        field.field_type = field_type
        return field
    return create_mock_field

@pytest.fixture
def qs_factory(mocker):
    def _qs_factory(size, ordered=True):
        mock_qs = mocker.MagicMock(spec=models.QuerySet)
        mock_qs.ordered = ordered
        if size == 0:
            mock_qs.__iter__.return_value = []
            mock_qs.count.return_value = 0
        else:
            mock_qs.__iter__.return_value = iter([i for i in range(1, size + 1)])
            mock_qs.__getitem__.side_effect = lambda x: [i for i in range(1, size + 1)][x]
            mock_qs.count.return_value = size
        return mock_qs
    return _qs_factory


@pytest.fixture
def bigquery_client_factory(mocker, mock_field_factory):
    def _factory(table_name, fields):
        # Convert any string fields to mock field objects
        processed_fields = []
        for field in fields:
            if isinstance(field, str):
                processed_fields.append(mock_field_factory(field, 'STRING'))
            else:
                processed_fields.append(field)

        # If no fields provided, create default fields
        if not processed_fields:
            processed_fields = [mock_field_factory(f'field_{x}', 'STRING') for x in range(3)]

        # Create a mock table
        table = mocker.MagicMock(table_name=table_name, schema=processed_fields)

        # Create a mock client that implements BigQueryClientInterface
        client = mocker.MagicMock(spec=BigQueryClientInterface)
        client.get_table.return_value = table

        # Configure the query method to return a job with a result method
        query_job = mocker.MagicMock()
        query_job.result.return_value = [(0,)]  # Default return value for count queries
        client.query.return_value = query_job

        return client

    return _factory


@pytest.fixture
def mock_model(mocker):
    model = mocker.MagicMock(spec=models.Model)
    queryset = mocker.MagicMock(spec=models.QuerySet)
    queryset.iterator.return_value = iter([])
    model.objects = mocker.MagicMock(spec=models.Manager)
    model.objects.all.return_value = queryset
    return model


@pytest.fixture
def test_exporter_factory(mocker, mock_model, qs_factory, bigquery_client_factory):
    def create_test_exporter(qs_size=5, table='test_table', batch_size=1000, qs_ordered=True, client=None, replace_nulls=False):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = table
            batch = batch_size
            replace_nulls_with_empty = replace_nulls

        if client is None:
            client = bigquery_client_factory(table, [])

        exporter = TestExporter(client=client)
        exporter.define_queryset = mocker.MagicMock()
        exporter.define_queryset.return_value = qs_factory(qs_size, qs_ordered)
        exporter._process_queryset = mocker.MagicMock()
        exporter._push_to_bigquery = mocker.MagicMock()
        return exporter

    return create_test_exporter