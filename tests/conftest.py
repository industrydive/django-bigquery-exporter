import pytest

from django.db import models
from google.cloud import bigquery


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
def bigquery_client_factory(mocker):
    def _factory(table_name, fields):
        schema = [bigquery.SchemaField(field, 'STRING') for field in fields]

        # Mocking the BigQuery client
        client = mocker.Mock(spec=bigquery.Client)
        # Mocking the table and schema
        table = mocker.MagicMock(table_name=table_name, schema=schema)
        # Setting up the client to return the mocked table
        client.get_table.return_value = table
        # Setup mock query functionality
        mock_query_job = mocker.MagicMock()
        mock_result = mocker.MagicMock()
        mock_result.__iter__.return_value = [(0,)]  # Default to empty table
        mock_query_job.result.return_value = mock_result
        client.query.return_value = mock_query_job
        # Setup insert_rows to return empty list (no errors)
        client.insert_rows.return_value = []

        return client

    return _factory


@pytest.fixture
def mock_client(mocker):
    client = mocker.Mock(spec=bigquery.Client)
    table = mocker.MagicMock()
    table.schema = [mocker.MagicMock(name='field1'), mocker.MagicMock(name='field2')]
    client.get_table.return_value = table

    # Setup mock query functionality
    mock_query_job = mocker.MagicMock()
    mock_result = mocker.MagicMock()
    mock_result.__iter__.return_value = [(0,)]  # Default to empty table
    mock_query_job.result.return_value = mock_result
    client.query.return_value = mock_query_job

    # Setup insert_rows to return empty list (no errors)
    client.insert_rows.return_value = []

    return client


@pytest.fixture
def mock_model(mocker):
    model = mocker.MagicMock(spec=models.Model)
    queryset = mocker.MagicMock(spec=models.QuerySet)
    queryset.iterator.return_value = iter([])
    model.objects = mocker.MagicMock(spec=models.Manager)
    model.objects.all.return_value = queryset
    return model


@pytest.fixture
def mock_client_factory(mocker, mock_client):
    """
    Fixture that provides a mock client factory for testing dependency injection.

    Returns a MagicMock that has a create_client method which returns the mock_client.
    """
    factory = mocker.MagicMock()
    factory.create_client.return_value = mock_client
    return factory
