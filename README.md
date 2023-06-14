# Django BigQuery Exporter

This Django application provides a convenient way to export data from your Django models to Google BigQuery.

## Features

* Batches data to avoid overloading memory
* Handles potential exceptions during data export
* Easy customization of fields to export

## Dependencies

- Python 3.8+
- Django
- google-cloud-bigquery
- google-api-python-client

## Setup

1. Install the package using pip:

```bash
pip install django-bigquery-exporter
```

2. Add your Google Cloud credentials to your environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
```

## Usage

Define your own exporter by inheriting from the `BigQueryExporter` class and defining the necessary attributes:

```python
from your_module import BigQueryExporter

class MyExporter(BigQueryExporter):
    model = MyModel
    fields = ['field1', 'field2']
    custom_fields = ['method1']
    batch = 1000
    table_name = 'my_table'

    def method1(self, obj):
        return obj.field1 + obj.field2
```

Then, simply create an instance of your exporter and call the `export` method to start the export:

```python
exporter = MyExporter()
exporter.export()
```

## Testing

To run the tests, simply use the command:

```bash
pytest
```

This project uses pytest for testing.

## Contribution

We welcome contributions to this project. Please feel free to open a pull request or create an issue on the GitHub page.
