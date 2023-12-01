import datetime
import logging
from uuid import UUID
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError

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

    Usage:
        # Make sure to order your queryset
        article_qs = Article.objects.order_by('id')
        for start, end, total, qs in batch_qs(article_qs):
            print "Now processing %s - %s of %s" % (start + 1, end, total)
            for article in qs:
                print article.body
    """
    total = qs.count()
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        yield (start, end, total, qs[start:end])


class BigQueryExporter:
    model = None
    fields = []
    batch = 1000
    table_name = ''
    replace_nulls_with_empty = False

    def __init__(self, project=None, credentials=None):
        assert self.model is not None, 'Model is not defined'
        assert self.table_name != '', 'BigQuery table name is not defined'
        self._initialize_client(project, credentials)
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

    def export(self, pull_date=None):
        """
        Export data to BigQuery.

        Args:
            pull_date (datetime.datetime, optional): The datetime used to populate the pull_date field.
                If not provided, the current date and time will be used.

        Raises:
            Exception: If an error occurs while exporting the data.

        Returns:
            None
        """
        pull_time = datetime.datetime.now() if not pull_date else pull_date
        try:
            queryset = self.define_queryset()
            for start, end, total, qs in batch_qs(queryset, self.batch):
                logger.info(f'Processing {start} - {end} of {total} {self.model}')
                reporting_data = self._process_queryset(qs, pull_time)
                if reporting_data:
                    self._push_to_bigquery(reporting_data)
            logger.info(
                f'Finished exporting {len(queryset)} {self.model} in {datetime.datetime.now() - pull_time}'
                )
        except Exception as e:
            logger.error(f'Error while exporting {self.model}: {e}')


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

    def _push_to_bigquery(self, data):
        try:
            errors = self.client.insert_rows(self.table, data)
            if errors:
                logger.error(f'Encountered errors while pushing to BigQuery {self.model}: {errors}')
        except GoogleAPICallError as e:
            logger.error(f'Error while exporting {self.model}: {e}')

    def _process_queryset(self, queryset, pull_time):
        processed_queryset = []
        for model_instance in queryset:
            processed_dict = {'pull_date': pull_time.strftime('%Y-%m-%d %H:%M:%S')}
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

        Args:
            value: The value to be sanitized.
        Returns:
            The sanitized value.

        """
        if isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
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
                raise Exception(
                    f'Invalid field {field} for model {self.model}. Must be a model field or a custom field method.'
                )

    def _validate_fields_against_table(self):
        """
        Validates that the fields defined in the exporter match the fields in the BigQuery table.
        """
        table_fields = [field.name for field in self.table.schema]
        for field in self.fields:
            if field not in table_fields:
                raise Exception(f'Field {field} is not in the BigQuery table {self.table_name}')
