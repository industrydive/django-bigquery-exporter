import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
import logging


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
    custom_fields = []
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
        return self.model.objects.all()

    def export(self):
        pull_time = datetime.datetime.now()
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
            processed_dict = {}
            processed_dict['pull_date'] = pull_time.strftime('%Y-%m-%d %H:%M:%S')
            for field in self.fields:
                processed_dict[field] = getattr(obj, field)
            for field in self.custom_fields:
                if hasattr(self, field):
                    processed_dict[field] = getattr(self, field)(obj)
            processed_queryset.append(processed_dict)
        return processed_queryset
