import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
import logging

def custom_field(function):
    """
    Decorator to mark a method as a custom field for a BigQueryExporter subclass.
    """
    function.is_custom_field = True
    return function


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

    def __init__(self):
        assert self.model is not None, 'Model is not defined'
        assert self.table_name != '', 'BigQuery table name is not defined'

        try:
            self.client = bigquery.Client()
            self.table = self.client.get_table(self.table_name)
        except GoogleAPICallError as e:
            logging.error(f'Error while creating BigQuery client: {e}')

        for field in self.custom_fields:
            if not hasattr(self, field):
                raise ValueError(f'Custom field {field} is not defined')

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

    def export(self, pull_date=None, *args, **kwargs):
        """
        Export data to BigQuery.

        Args:
            pull_date (datetime.datetime, optional): The datetime used to populate the pull_date field. If not provided, the current date and time will be used.

        Raises:
            Exception: If an error occurs while exporting the data.

        Returns:
            None
        """
        pull_time = datetime.datetime.now() if not pull_date else pull_date
        try:
            queryset = self.define_queryset()
            for start, end, total, qs in batch_qs(queryset, self.batch):
                print(f'Processing {start} - {end} of {total} {self.model}')
                reporting_data = self._process_queryset(qs, pull_time)
                if reporting_data:
                    self._push_to_bigquery(reporting_data)
        except Exception as e:
            logging.error(f'Error while exporting {self.model}: {e}')
        finally:
            print(f'Finished exporting {len(queryset)} {self.model} in {datetime.datetime.now() - pull_time}')

    def _push_to_bigquery(self, data):
        try:
            errors = self.client.insert_rows(self.table, data)
            if errors:
                logging.error(f'Encountered errors while exporting {self.model}: {errors}')
        except GoogleAPICallError as e:
            logging.error(f'Error while exporting {self.model}: {e}')

    def _process_queryset(self, queryset, pull_time):
        processed_queryset = []
        for obj in queryset:
            processed_dict = {'pull_date': pull_time.strftime('%Y-%m-%d %H:%M:%S')}
            for field in self.fields:
                if hasattr(self, field):
                    if callable(getattr(self, field)) and getattr(getattr(self, field), 'is_custom_field', False):
                        # If the field is a custom field method
                        processed_dict[field] = getattr(self, field)(obj)
                else:
                    # Regular field
                    processed_dict[field] = getattr(obj, field)
            processed_queryset.append(processed_dict)
        return processed_queryset
