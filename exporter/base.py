import datetime
from django.core import serializers
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


# override django serializer to export jsonl with datetime as string of format YYYY-MM-DD HH:MM:SS
class BigQuerySerializer(serializers.json.Serializer):
    def handle_field(self, obj, field):
        value = field.value_from_object(obj)
        if isinstance(value, datetime.datetime):
            value = value.strftime('%Y-%m-%d %H:%M:%S')
        return value


class BigQueryExporter:
    model = None
    fields = []
    custom_fields = []
    batch = 5000
    table_name = ''

    def __init__(self):
        assert self.model is not None, 'Model is not defined'
        assert self.table_name != '', 'BigQuery table name is not defined'

        try:
            self.client = bigquery.Client()
        except GoogleAPICallError as e:
            logging.error(f'Error while creating BigQuery client: {e}')

        self.queryset = self.define_queryset()

        if not self.table_exists():
            raise ValueError('Table %s does not exist' % self.table_name)

    def define_queryset(self):
        return self.model.objects.all()

    def export(self):
        pull_time = datetime.datetime.now()
        try:
            for start, end, total, qs in batch_qs(self.queryset, self.batch):
                print(f'Processing {start} - {end} of {total} {self.model}')
                reporting_data = serializers.serialize('jsonl', qs, fields=self.fields, cls=BigQuerySerializer)
                if reporting_data:
                    for obj in reporting_data:
                        for field in self.custom_fields:
                            obj['fields'][field.__name__] = field(obj)
                        try:
                            self.client.insert_rows_json(self.table_name, [obj['fields']], row_ids=[None] * len(obj['fields']))
                        except Exception as e:
                            logging.error(f'Error while exporting {obj["pk"]}: {e}')
        except Exception as e:
            logging.error(f'Error while exporting {self.model}: {e}')
        finally:
            print(f'Finished exporting {self.model} in {datetime.datetime.now() - pull_time}')

    def table_exists(self):
        try:
            self.client.get_table(self.table_name)
            return True
        except Exception as e:
            logging.error(f'Error while checking table {self.table_name}: {e}')
            return False
