=====
Django BigQuery Exporter
=====

Django BigQuery Exporter is a Django app to export data to Google BigQuery.

Quick start
-----------

1. Add "django-bigquery-exporter" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'django-bigquery-exporter',
    ]

2. Add the following settings to your settings.py file:

    BIGQUERY_EXPORTER = {
        'DATASET': 'my_dataset',
        'CREDENTIALS': '/path/to/credentials.json',
  }

3. Create a subclass of `BigQueryExporter` and define the `define_queryset` method:

    from django_bigquery_exporter import BigQueryExporter

    class MyExporter(BigQueryExporter):
        def define_queryset(self):
            return MyModel.objects.all()