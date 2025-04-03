import pytest
import datetime
from google.api_core.exceptions import GoogleAPICallError, RetryError

from bigquery_exporter.base import batch_qs, custom_field, BigQueryExporter, BigQueryClientFactory


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


class TestBigQueryClientFactory:
    def test_create_client_with_default_settings(self, mocker):
        # Mock the bigquery.Client to avoid actual API calls
        mock_client = mocker.patch('bigquery_exporter.base.bigquery.Client')

        # Call the factory method
        BigQueryClientFactory.create_client()

        # Verify the client was created with default settings
        mock_client.assert_called_once_with(project=None)

    def test_create_client_with_project(self, mocker):
        mock_client = mocker.patch('bigquery_exporter.base.bigquery.Client')

        BigQueryClientFactory.create_client(project='test-project')

        mock_client.assert_called_once_with(project='test-project')

    def test_create_client_with_credentials(self, mocker):
        mock_client = mocker.patch('bigquery_exporter.base.bigquery.Client')
        mock_credentials = mocker.patch('bigquery_exporter.base.service_account.Credentials.from_service_account_file')
        mock_credentials.return_value = 'mock_creds'

        BigQueryClientFactory.create_client(credentials='path/to/credentials.json')

        mock_credentials.assert_called_once_with('path/to/credentials.json')
        mock_client.assert_called_once_with(project=None, credentials='mock_creds')

    def test_create_client_handles_error(self, mocker):
        mock_client = mocker.patch('bigquery_exporter.base.bigquery.Client')
        mock_client.side_effect = GoogleAPICallError('API Error', '')

        with pytest.raises(GoogleAPICallError):
            BigQueryClientFactory.create_client()


@pytest.fixture
def test_exporter_factory(mocker, mock_client, mock_model, qs_factory):
    def create_test_exporter(qs_size=5, table='test_table', batch_size=1000, qs_ordered=True):
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = table
            batch = batch_size

        exporter = TestExporter(client=mock_client)  # Use dependency injection instead of patching
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

    def test_init_with_custom_client_factory(self, mocker, mock_model):
        # Create a custom client factory
        custom_factory = mocker.MagicMock()
        mock_client = mocker.MagicMock()
        mock_client.get_table.return_value = mocker.MagicMock(schema=[])
        custom_factory.create_client.return_value = mock_client

        # Initialize exporter with custom factory
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = []

        exporter = TestExporter(client_factory=custom_factory)

        # Verify the custom factory was used
        custom_factory.create_client.assert_called_once()
        assert exporter.client == mock_client

    def test_init_with_direct_client_injection(self, mocker, mock_model):
        # Create a mock client
        mock_client = mocker.MagicMock()
        mock_client.get_table.return_value = mocker.MagicMock(schema=[])

        # Initialize exporter with direct client injection
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = []

        exporter = TestExporter(client=mock_client)

        # Verify the injected client is used directly without creating a new one
        assert exporter.client == mock_client

    def test_init_client_factory_precedence(self, mocker, mock_model):
        """Test that providing both client and client_factory uses the client."""
        mock_client = mocker.MagicMock()
        mock_client.get_table.return_value = mocker.MagicMock(schema=[])

        mock_factory = mocker.MagicMock()

        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = []

        exporter = TestExporter(client=mock_client, client_factory=mock_factory)

        # The factory should not be called when a client is directly provided
        mock_factory.create_client.assert_not_called()
        assert exporter.client == mock_client

    def test_custom_field_decorator_sets_custom_attribute_on_callable(self):
        @custom_field
        def test_field(self, obj):
            pass

        assert test_field.is_custom_field

    def test_custom_field_succeeds_during_processing(self, bigquery_client_factory, mocker, mock_model):
        mock_model.field_value = 1
        # mock the client to return a table with the field 'field_value' and 'custom_field'
        mock_client = bigquery_client_factory('test_table', ['field_value', 'custom_field'])

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = ['field_value', 'custom_field']

            @custom_field
            def custom_field(self, obj):
                return obj.field_value * 2

        # Use dependency injection instead of mocking the Client constructor
        exporter = TestBigQueryExporter(client=mock_client)
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

    def test_end_to_end_with_dependency_injection(self, mocker, mock_model, bigquery_client_factory):
        """Test the entire export process using dependency injection pattern for easier testing."""
        # Setup test data
        mock_model.id = 100
        mock_model.name = "Test Item"
        mock_model.date_created = datetime.datetime(2023, 1, 1)

        # Create a queryable mock
        mock_queryset = mocker.MagicMock()
        mock_queryset.count.return_value = 1
        mock_queryset.__getitem__.return_value = [mock_model]
        mock_queryset.__iter__.return_value = [mock_model]

        # Setup all method to return our queryable mock
        mock_all = mocker.patch.object(mock_model.objects, 'all')
        mock_all.return_value.order_by.return_value = mock_queryset

        # Create a test table with appropriate schema
        mock_client = bigquery_client_factory('test.table', ['id', 'name', 'date_created', 'pull_date'])

        # Setup our exporter class with a custom field
        class TestExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test.table'
            fields = ['id', 'name', 'date_created']

        # Use dependency injection to create the exporter with our mock client
        exporter = TestExporter(client=mock_client)

        # Run the export
        errors = exporter.export(pull_date=datetime.datetime(2023, 1, 15))

        # Verify results
        assert errors == []

        # Check mock_client.insert_rows was called with the correct data
        args, _ = mock_client.insert_rows.call_args
        inserted_data = args[1]

        assert len(inserted_data) == 1
        assert inserted_data[0]['id'] == 100
        assert inserted_data[0]['name'] == "Test Item"
        assert inserted_data[0]['date_created'] == "2023-01-01 00:00:00"
        assert inserted_data[0]['pull_date'] == "2023-01-15 00:00:00"
