import datetime
import logging
from uuid import UUID
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.exceptions import RetryError
from google.api_core.retry import Retry

from bigquery_exporter.errors import BigQueryExporterInitError, BigQueryExporterValidationError
from bigquery_exporter.helpers import handle_datetime_value

from django.db.models import QuerySet

logger = logging.getLogger(__name__)


def custom_field(method):
    """
    Decorator to mark a method as a custom field for a BigQueryExporter subclass.
    """
    # Ensure that the method has exactly two arguments: self and the Django model instance
    assert method.__code__.co_argcount == 2, \
        'Custom field methods must have exactly two arguments: self and the Django model instance'
    method.is_custom_field = True
    return method


def batch_qs(qs, batch_size=1000):
    """
    Returns a (start, end, total, queryset) tuple for each batch in the given
    queryset. Allows for fetching of large querysets in batches without loading
    the entire queryset into memory at once.

    Args:
        qs (django.db.models.QuerySet): The Django QuerySet to batch.
        batch_size (int, optional): The size of each batch. Defaults to 1000.
                                    If None, the entire queryset is returned in a single batch.

    Usage:
        # Make sure to order your queryset
        article_qs = Article.objects.order_by('id')
        for start, end, total, qs in batch_qs(article_qs):
            print "Now processing %s - %s of %s" % (start + 1, end, total)
            for article in qs:
                print article.body
    """
    total = qs.count()

    if batch_size is None:
        yield (0, total, total, qs)
    else:
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            yield (start, end, total, qs[start:end])


class BigQueryExporter:
    """
    Base class for exporting Django models to BigQuery.
    """
    model = None
    fields = []
    batch = 1000
    table_name = ''
    replace_nulls_with_empty = False
    include_pull_date = False
    pull_date_field_name = 'pull_date'

    def __init__(self, project=None, credentials=None):
        """
        Initializes the BigQueryExporter.

        Args:
            project (str, optional): The Google Cloud project id. If not provided, the default project
                will be used.
            credentials (str, optional): The path to the service account credentials file. If not provided,
                the default credentials will be used.

        Raises:
            BigQueryExporterInitError: If an error occurs while initializing the BigQuery client.
            BigQueryExporterValidationError: If an error occurs while validating the fields.
            AssertionError: If the model or table_name is not defined.
        """
        assert self.model is not None, 'Model is not defined'
        assert self.table_name != '', 'BigQuery table name is not defined'
        logger.info(f'Initializing BigQuery client for {self.__class__.__name__}')
        self._initialize_client(project, credentials)
        logger.info(f'Validating fields for {self.__class__.__name__}')
        self._validate_fields()

    def define_queryset(self):
        """
        Returns the queryset for exporting data to BigQuery.

        This method can be overridden in subclasses to specify additional filters or ordering
        for the queryset. For example, you can use this method to filter data based on date ranges
        or status fields.

        It is important to note that if the queryset is larger than the class's batch size,
        it must be ordered to avoid ordering bugs when accessing different segments of the queryset.

        Returns:
            QuerySet: The queryset for exporting data to BigQuery.
        """
        return self.model.objects.all().order_by('id')

    def export(self, pull_date=None, queryset=None):
        """
        Export data to BigQuery.

        Args:
            pull_date (datetime.datetime, optional): The datetime used to populate the pull_date field.
                If not provided, the current date and time will be used.
            queryset (django.db.models.QuerySet, optional): A Django QuerySet to export.
                If not provided, the method will use a default queryset defined by `define_queryset()`.

        Raises:
            TypeError: If `pull_date` is provided but is not an instance of datetime.datetime,
                       or if `queryset` is provided but is not an instance of django.db.models.QuerySet.
            GoogleAPICallError: If an error occurs during the Google API call while exporting data.
            RetryError: If a retryable error occurs during the export process.
            Exception: If an error occurs while exporting the data.

        Returns:
            errors: A list of errors that occurred while exporting the data.
        """
        # Set default values
        pull_datetime = datetime.datetime.now() if not pull_date else pull_date
        queryset = self.define_queryset() if not queryset else queryset

        # Validate pull_date type if provided (not None)
        if pull_date is not None and not isinstance(pull_date, datetime.datetime):
            raise TypeError(f'Expected a datetime.datetime object for pull_date, but got {type(pull_date).__name__} instead.')

        # Validate queryset type before entering the try block
        if not isinstance(queryset, QuerySet):
            raise TypeError(f'Expected a Django QuerySet, but got {type(queryset).__name__} instead.')

        # Ensure queryset is ordered when batch size is not None and queryset size is larger than batch
        if (self.batch is not None) and (queryset.count() > self.batch) and not queryset.ordered:
            raise ValueError('Queryset must be ordered (using .order_by()) when batch size '
                             f'({self.batch}) is smaller than queryset size ({queryset.count()}).')

        # Handle the export process with runtime errors
        errors = []
        try:
            for start, end, total, qs in batch_qs(queryset, self.batch):
                logger.info(f'Processing {start} - {end} of {total} {self.model}')
                if reporting_data := self._process_queryset(qs, pull_datetime):
                    if batch_errors := self._push_to_bigquery(reporting_data):
                        # updating the row index to account for the batch offset
                        for error in batch_errors:
                            error['index'] += start
                        errors.extend(batch_errors)

            logger.info(f'Finished exporting {len(queryset)} {self.model} in {datetime.datetime.now() - pull_datetime}')
        except (GoogleAPICallError, RetryError) as e:
            logger.error(f'GoogleAPIError while exporting {self.__class__.__name__}: {e}')
            raise e
        return errors

    def table_has_data(self, pull_date=None):
        """
        Checks if the BigQuery table has data for the given pull_date.

        Args:
            pull_date (datetime.datetime, optional): The pull_date to check for. If not provided,
                the current date and time will be used.

        Returns:
            bool: True if the table has data for the given pull_date, False otherwise.
        """
        if pull_date and self.include_pull_date:
            # Convert pull_date to UTC if it has timezone info
            if pull_date.tzinfo is not None:
                utc_date = pull_date.astimezone(pytz.UTC)
            else:
                # If naive datetime (no timezone), assume it's in UTC
                utc_date = pytz.UTC.localize(pull_date)

            query = f'SELECT COUNT(*) FROM {self.table_name} WHERE DATE({self.pull_date_field_name}) = "{utc_date.strftime("%Y-%m-%d")}"'
        else:
            query = f'SELECT COUNT(*) FROM {self.table_name}'
        query_job = self.client.query(query)
        results = query_job.result()
        for row in results:
            return row[0] > 0
        return False

    def _initialize_client(self, project=None, credentials=None):
        try:
            if credentials:  # use service account credentials if provided
                service_account_info = service_account.Credentials.from_service_account_file(credentials)
                self.client = bigquery.Client(project=project, credentials=service_account_info)
            else:  # otherwise, use default credentials
                self.client = bigquery.Client(project=project)
            self.table = self.client.get_table(self.table_name)
        except GoogleAPICallError as e:
            logging.error(f'Error while creating BigQuery client: {e}')
            raise BigQueryExporterInitError(e)

    def _push_to_bigquery(self, data, retry_deadline=600):
        try:
            if errors := self.client.insert_rows(self.table, data, retry=Retry(deadline=retry_deadline)):
                return errors
        except (GoogleAPICallError, RetryError) as e:
            logger.error(f'Error pushing {self.__class__.__name__} to {self.table_name}: {e}')
            raise e

    def _process_queryset(self, queryset, pull_datetime):
        processed_queryset = []
        for model_instance in queryset:
            processed_dict = {}
            if self.include_pull_date:
                processed_dict[self.pull_date_field_name] = self._sanitize_value(pull_datetime)
            for field in self.fields:
                processed_dict[field] = self._process_field(model_instance, field)
            processed_queryset.append(processed_dict)
        return processed_queryset

    def _process_field(self, model_instance, field):
        exporter_field = getattr(self, field, None)
        if callable(exporter_field) and getattr(exporter_field, 'is_custom_field', False):
            return exporter_field(model_instance)
        else:
            model_field = getattr(model_instance, field)
            return self._sanitize_value(model_field)

    def _sanitize_value(self, value):
        """
        Sanitizes db values to be BigQuery compliant. Converts datetimes and UUIDs to strings.
        Checks for null values and replaces them with empty strings if replace_nulls_with_empty is True.
        Ensures all datetime objects are in UTC before converting to strings.

        Args:
            value: The value to be sanitized.
        Returns:
            The sanitized value.

        """
        if isinstance(value, datetime.datetime):
            # Convert datetime to UTC if it has timezone info
            handle_datetime_value(value)
        elif isinstance(value, UUID):
            return str(value)
        elif value is None:
            return '' if self.replace_nulls_with_empty else None
        else:
            return value

    def _validate_fields(self):
        """
        Validates the fields defined in the exporter.
        """
        self._validate_fields_against_model()
        self._validate_fields_against_table()

    def _validate_fields_against_model(self):
        for field in self.fields:
            # check that all fields are valid (either a model field or a custom field method)
            if not hasattr(self.model, field) and not hasattr(self, field):
                raise BigQueryExporterValidationError(
                    f'Invalid field {field} for model {self.model}. Must be a model field or a custom field method.'
                )

    def _validate_fields_against_table(self):
        """
        Validates that the fields defined in the exporter match the fields in the BigQuery table.
        """
        table_fields = [field.name for field in self.table.schema]
        for field in self.fields:
            if field not in table_fields:
                raise BigQueryExporterValidationError(f'Field {field} is not in the BigQuery table {self.table_name}')
