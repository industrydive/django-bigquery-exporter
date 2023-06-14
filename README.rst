===============================
Django BigQuery Exporter
===============================

Django BigQuery Exporter is simple library providing an admin-esque base class for batching and exporting Django models to Google BigQuery.

Quick start
-----------
1. Install Django BigQuery Exporter: ``pip install django-bigquery-exporter``


2. Make sure you have your Google Cloud credentials set up. See [here](https://cloud.google.com/docs/authentication/getting-started) for more information.

3. Import and create a subclass of `BigQueryExporter` and define the `define_queryset` method
::

    from bigquery_exporter.base import BigQueryExporter

    class MyExporter(BigQueryExporter):
        def define_queryset(self):
            return MyModel.objects.all()

4. Call the `export` method
::

    exporter = MyExporter()
    exporter.export()


Et voila! Your data is now in BigQuery.