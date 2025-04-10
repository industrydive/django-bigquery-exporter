import pytest
import datetime
import pytz
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
            assert 'pull_date' not in processed  # pull_date should not be included by default

    def test_process_queryset_with_pull_date(self, bigquery_client_factory, mocker, mock_model):
        mock_model.field_value = 1
        # mock the client to return a table with the field 'field_value' and 'pull_date'
        mock_client = bigquery_client_factory('test_table', ['field_value', 'pull_date'])
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = ['field_value']
            include_pull_date = True

        exporter = TestBigQueryExporter()
        mock_queryset = [mock_model]
        pull_time = datetime.datetime(2023, 1, 1, 0, 0, 0)
        processed_data = exporter._process_queryset(mock_queryset, pull_time)

        assert len(processed_data) == len(mock_queryset)
        for original, processed in zip(mock_queryset, processed_data):
            assert processed['field_value'] == 1
            assert processed['pull_date'] == pull_time.strftime('%Y-%m-%d %H:%M:%S')

    def test_process_queryset_with_custom_pull_date_name(self, bigquery_client_factory, mocker, mock_model):
        mock_model.field_value = 1
        # mock the client to return a table with the field 'field_value' and 'export_time'
        mock_client = bigquery_client_factory('test_table', ['field_value', 'export_time'])
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = ['field_value']
            include_pull_date = True
            pull_date_field_name = 'export_time'

        exporter = TestBigQueryExporter()
        mock_queryset = [mock_model]
        pull_time = datetime.datetime(2023, 1, 1, 0, 0, 0)
        processed_data = exporter._process_queryset(mock_queryset, pull_time)

        assert len(processed_data) == len(mock_queryset)
        for original, processed in zip(mock_queryset, processed_data):
            assert processed['field_value'] == 1
            assert processed['export_time'] == pull_time.strftime('%Y-%m-%d %H:%M:%S')
            assert 'pull_date' not in processed

    def test_table_has_data_with_pull_date_disabled(self, test_exporter):
        """Test table_has_data when include_pull_date is False"""
        # Run with a pull_date but include_pull_date=False
        pull_date = datetime.datetime(2023, 1, 1)
        test_exporter.include_pull_date = False
        test_exporter.table_has_data(pull_date)
        # Should call with a basic query without pull_date filter
        test_exporter.client.query.assert_called_with(f'SELECT COUNT(*) FROM {test_exporter.table_name}')

    def test_table_has_data_with_pull_date_enabled(self, test_exporter):
        """Test table_has_data when include_pull_date is True"""
        # Run with a pull_date and include_pull_date=True
        pull_date = datetime.datetime(2023, 1, 1)
        test_exporter.include_pull_date = True
        test_exporter.table_has_data(pull_date)
        # Should call with a query that filters by pull_date
        expected_query = f'SELECT COUNT(*) FROM {test_exporter.table_name} WHERE DATE(pull_date) = "2023-01-01"'
        test_exporter.client.query.assert_called_with(expected_query)

    def test_table_has_data_with_custom_pull_date_name(self, test_exporter):
        """Test table_has_data with custom pull_date_field_name"""
        # Run with a pull_date, include_pull_date=True, and custom pull_date_field_name
        pull_date = datetime.datetime(2023, 1, 1)
        test_exporter.include_pull_date = True
        test_exporter.pull_date_field_name = 'export_time'
        test_exporter.table_has_data(pull_date)
        # Should call with a query that filters by the custom field name
        expected_query = f'SELECT COUNT(*) FROM {test_exporter.table_name} WHERE DATE(export_time) = "2023-01-01"'
        test_exporter.client.query.assert_called_with(expected_query)

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

    def test_sanitize_value_converts_naive_datetime_to_utc(self, bigquery_client_factory, mocker, mock_model):
        """Test that naive datetime objects are converted to UTC before stringifying"""
        # mock the client to return a table with the required fields
        mock_client = bigquery_client_factory('test_table', ['field_value'])
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = []

        exporter = TestBigQueryExporter()

        # Create a naive datetime
        naive_dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        result = exporter._sanitize_value(naive_dt)

        # The result should be a string in the correct format
        assert isinstance(result, str)

        # The result should be the same time in UTC
        expected = pytz.UTC.localize(naive_dt).strftime('%Y-%m-%d %H:%M:%S')
        assert result == expected

    def test_sanitize_value_preserves_aware_datetime_timezone(self, bigquery_client_factory, mocker, mock_model):
        """Test that timezone-aware datetime objects are correctly converted to UTC"""
        # mock the client to return a table with the required fields
        mock_client = bigquery_client_factory('test_table', ['field_value'])
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = []

        exporter = TestBigQueryExporter()

        # Create a timezone-aware datetime (UTC+2)
        # Using pytz to create a timezone-aware datetime
        utc_plus_2 = pytz.timezone('Europe/Helsinki')  # UTC+2 or +3 depending on DST
        aware_dt = utc_plus_2.localize(datetime.datetime(2023, 1, 1, 12, 0, 0))  # 12:00 UTC+2
        result = exporter._sanitize_value(aware_dt)

        # Should be 10:00 UTC (or 9:00 depending on DST)
        utc_time = aware_dt.astimezone(pytz.UTC)
        expected = utc_time.strftime('%Y-%m-%d %H:%M:%S')
        assert result == expected

    def test_process_queryset_applies_utc_conversion_to_pull_date(self, bigquery_client_factory, mocker, mock_model):
        """Test that pull_date is properly converted to UTC in _process_queryset"""
        mock_model.field_value = 1
        # mock the client to return a table with the required fields
        mock_client = bigquery_client_factory('test_table', ['field_value', 'pull_date'])
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        class TestBigQueryExporter(BigQueryExporter):
            model = mock_model
            table_name = 'test_table'
            fields = []
            include_pull_date = True

        exporter = TestBigQueryExporter()
        mock_queryset = [mock_model]

        # Create a timezone-aware datetime (UTC+2)
        utc_plus_2 = pytz.timezone('Europe/Helsinki')  # UTC+2 or +3 depending on DST
        pull_time = utc_plus_2.localize(datetime.datetime(2023, 1, 1, 12, 0, 0))  # 12:00 UTC+2

        processed_data = exporter._process_queryset(mock_queryset, pull_time)

        # Pull date should be converted to UTC
        utc_time = pull_time.astimezone(pytz.UTC)
        expected = utc_time.strftime('%Y-%m-%d %H:%M:%S')
        assert processed_data[0]['pull_date'] == expected

    def test_table_has_data_converts_timezone_aware_datetime(self, test_exporter):
        """Test table_has_data correctly converts timezone-aware datetime to UTC"""
        # Create a timezone-aware datetime (UTC+2)
        utc_plus_2 = pytz.timezone('Europe/Helsinki')  # UTC+2 or +3 depending on DST
        pull_date = utc_plus_2.localize(datetime.datetime(2023, 1, 1, 12, 0, 0))  # 12:00 UTC+2

        test_exporter.include_pull_date = True
        test_exporter.table_has_data(pull_date)

        # Should query with UTC date (Jan 1, not Jan 1 + timezone offset)
        expected_query = f'SELECT COUNT(*) FROM {test_exporter.table_name} WHERE DATE(pull_date) = "2023-01-01"'
        test_exporter.client.query.assert_called_with(expected_query)

    def test_sanitize_value_with_type_specific_defaults(self, bigquery_client_factory, mocker, mock_field_factory):
        """Test _sanitize_value with replace_nulls_with_empty=True and different field types"""
        # Create mock schema fields with proper attributes
        string_field = mock_field_factory('string_field', 'STRING')
        integer_field = mock_field_factory('integer_field', 'INTEGER')
        float_field = mock_field_factory('float_field', 'FLOAT')
        boolean_field = mock_field_factory('boolean_field', 'BOOLEAN')
        json_field = mock_field_factory('json_field', 'JSON')
        unknown_field = mock_field_factory('unknown_field', 'UNKNOWN_TYPE')

        # Create a mock table with fields of different types
        mock_table = mocker.MagicMock()
        mock_table.schema = [
            string_field,
            integer_field,
            float_field,
            boolean_field,
            json_field,
            unknown_field
        ]

        # Create a mock exporter with replace_nulls_with_empty=True
        mock_client = bigquery_client_factory('test_table', [
            string_field,
            integer_field,
            float_field,
            boolean_field,
            json_field,
            unknown_field
        ])

        class TestExporter(BigQueryExporter):
            model = mocker.MagicMock()
            table_name = 'test_table'
            fields = []
            replace_nulls_with_empty = True

        # Mock the client creation to avoid actual API calls
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        exporter = TestExporter()
        exporter.table = mock_table  # Set the table directly

        # Test with None values for different field types
        assert exporter._sanitize_value(None, 'string_field') == ''
        assert exporter._sanitize_value(None, 'integer_field') == 0
        assert exporter._sanitize_value(None, 'float_field') == 0.0
        assert exporter._sanitize_value(None, 'boolean_field') is False
        assert exporter._sanitize_value(None, 'json_field') == '{}'
        # Unknown type should default to empty string
        assert exporter._sanitize_value(None, 'unknown_field') == ''
        # Field not in schema should default to empty string
        assert exporter._sanitize_value(None, 'nonexistent_field') == ''

    def test_sanitize_value_with_none_values_disabled_replacement(self, mocker):
        """Test _sanitize_value with replace_nulls_with_empty=False"""
        # Create mock schema fields with proper attributes
        string_field = mocker.MagicMock()
        string_field.name = 'string_field'
        string_field.field_type = 'STRING'

        integer_field = mocker.MagicMock()
        integer_field.name = 'integer_field'
        integer_field.field_type = 'INTEGER'

        # Create a mock table
        mock_table = mocker.MagicMock()
        mock_table.schema = [
            string_field,
            integer_field
        ]

        # Create a mock exporter with replace_nulls_with_empty=False
        mock_client = mocker.MagicMock()
        mock_client.get_table.return_value = mock_table

        class TestExporter(BigQueryExporter):
            model = mocker.MagicMock()
            table_name = 'test_table'
            fields = []
            replace_nulls_with_empty = False

        # Mock the client creation to avoid actual API calls
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        exporter = TestExporter()
        exporter.table = mock_table  # Set the table directly

        # Test with None values - should remain None
        assert exporter._sanitize_value(None, 'string_field') is None
        assert exporter._sanitize_value(None, 'integer_field') is None
        assert exporter._sanitize_value(None, 'nonexistent_field') is None

    def test_sanitize_value_with_non_none_values(self, mocker):
        """Test _sanitize_value with non-None values (should not be affected by type-specific defaults)"""
        # Create mock schema fields with proper attributes
        string_field = mocker.MagicMock()
        string_field.name = 'string_field'
        string_field.field_type = 'STRING'

        integer_field = mocker.MagicMock()
        integer_field.name = 'integer_field'
        integer_field.field_type = 'INTEGER'

        float_field = mocker.MagicMock()
        float_field.name = 'float_field'
        float_field.field_type = 'FLOAT'

        boolean_field = mocker.MagicMock()
        boolean_field.name = 'boolean_field'
        boolean_field.field_type = 'BOOLEAN'

        # Create a mock table
        mock_table = mocker.MagicMock()
        mock_table.schema = [
            string_field,
            integer_field,
            float_field,
            boolean_field
        ]

        # Create a mock exporter with replace_nulls_with_empty=True
        mock_client = mocker.MagicMock()
        mock_client.get_table.return_value = mock_table

        class TestExporter(BigQueryExporter):
            model = mocker.MagicMock()
            table_name = 'test_table'
            fields = []
            replace_nulls_with_empty = True

        # Mock the client creation to avoid actual API calls
        mocker.patch('bigquery_exporter.base.bigquery.Client', return_value=mock_client)

        exporter = TestExporter()
        exporter.table = mock_table  # Set the table directly

        # Test with non-None values - should remain unchanged
        assert exporter._sanitize_value("test", 'string_field') == "test"
        assert exporter._sanitize_value(42, 'integer_field') == 42
        assert exporter._sanitize_value(3.14, 'float_field') == 3.14
        assert exporter._sanitize_value(True, 'boolean_field') is True
