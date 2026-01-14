import pytest
from unittest.mock import MagicMock

from google.api_core.exceptions import GoogleAPICallError

from bigquery_exporter.clients.google_client import GoogleBigQueryClient
from bigquery_exporter.errors import BigQueryExporterInitError


class TestGoogleBigQueryClient:
    """
    Unit tests for GoogleBigQueryClient
    """
    def test_init_with_no_credentials_uses_adc(self, mocker):
        mock_bq_client_class = mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client')
        client = GoogleBigQueryClient(project='my-project')

        mock_bq_client_class.assert_called_once_with(project='my-project')
        assert client.client == mock_bq_client_class.return_value

    def test_init_with_str_credentials(self, mocker):
        project = 'fake-project'
        fake_path = '/path/to/fake-service-account.json'

        mock_bq_client_class = mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client')
        mock_from_file = mocker.patch('google.oauth2.service_account.Credentials.from_service_account_file')
        fake_creds = MagicMock()
        mock_from_file.return_value = fake_creds

        client = GoogleBigQueryClient(project=project, credentials=fake_path)

        mock_from_file.assert_called_once_with(fake_path)
        mock_bq_client_class.assert_called_once_with(project=project, credentials=fake_creds)
        assert client.client == mock_bq_client_class.return_value

    def test_init_with_existing_service_account_credentials(self, mocker, mock_service_account_credentials):
        project = 'fake-project'
        mock_bq_client_class = mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client')

        client = GoogleBigQueryClient(project=project, credentials=mock_service_account_credentials)

        mock_bq_client_class.assert_called_once_with(project=project, credentials=mock_service_account_credentials)
        assert client.client == mock_bq_client_class.return_value

    def test_init_raises_valueerror_on_unsupported_credentials_type(self):
        with pytest.raises(ValueError) as exc_info:
            GoogleBigQueryClient(
                project='fake-project',
                # Invalid credentials type
                credentials={'fake': 'service_account'}
            )

        assert 'Unsupported credentials type: "dict"' in str(exc_info.value)

    def test_init_raises_bigqueryexporteriniterror_on_google_api_error(self, mocker):
        mock_bq_client_class = mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client')
        mock_bq_client_class.side_effect = GoogleAPICallError('Project not found')

        with pytest.raises(BigQueryExporterInitError) as exc_info:
            GoogleBigQueryClient(project='nonexistent-project')

        assert 'Project not found' in str(exc_info.value)

    def test_get_table_delegates_to_client(self, mocker):
        mock_bq_client = MagicMock()
        mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client', return_value=mock_bq_client)
        client = GoogleBigQueryClient()
        fake_table_id = 'project.dataset.table'

        result = client.get_table(fake_table_id)

        mock_bq_client.get_table.assert_called_once_with(fake_table_id)
        assert result == mock_bq_client.get_table.return_value

    def test_insert_rows_delegates_to_client(self, mocker, fake_table):
        mock_bq_client = MagicMock()
        mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client', return_value=mock_bq_client)
        client = GoogleBigQueryClient()
        rows = [{'id': 1, 'value': 'test'}]

        client.insert_rows(fake_table, rows)

        mock_bq_client.insert_rows.assert_called_once_with(
            fake_table,
            rows,
            retry=None
        )

    def test_query_delegates_to_client(self, mocker):
        mock_bq_client = MagicMock()
        mocker.patch('bigquery_exporter.clients.google_client.bigquery.Client', return_value=mock_bq_client)
        client = GoogleBigQueryClient()
        sql = 'SELECT * FROM `project.dataset.table` LIMIT 10'

        job = client.query(sql)

        mock_bq_client.query.assert_called_once_with(sql)
        assert job == mock_bq_client.query.return_value