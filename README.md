# Django BigQuery Exporter

A Django application that provides a convenient way to export data from your Django models to Google BigQuery.

## Features

* Exports Django model data to BigQuery tables
* Processes data in configurable batch sizes to manage memory usage
* Handles date/time formats and UUID fields automatically
* Allows custom field transformations with a simple decorator
* Validates that model fields match BigQuery table schema
* Provides retry mechanisms for resilient exports
* Supports incremental exports with date filtering
* Handles potential exceptions during data export with detailed error reporting

## Installation

```bash
pip install django-bigquery-exporter
```

## Requirements

- Python 3.8+
- Django
- google-cloud-bigquery
- google-api-python-client

## Authentication

You need to authenticate with Google Cloud to use BigQuery. There are two main ways:

1. Using environment variables (recommended for production):
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
   ```

2. Providing credentials directly in code (useful for development):
   ```python
   exporter = MyExporter(
       project="your-google-cloud-project-id",
       credentials="/path/to/your/credentials.json"
   )
   ```

## Basic Usage

Create a subclass of `BigQueryExporter` and define the necessary attributes:

```python
from bigquery_exporter.base import BigQueryExporter, custom_field

class BookExporter(BigQueryExporter):
    model = Book
    fields = ['id', 'title', 'author', 'publication_date', 'genre', 'rating']
    batch = 1000
    table_name = 'your_project.your_dataset.books'
    replace_nulls_with_empty = False

    @custom_field
    def genre(self, instance):
        """Custom field to transform the genre into a structured format"""
        return {
            'code': instance.genre,
            'name': instance.get_genre_display()
        }
```

Then, export the data:

```python
exporter = BookExporter()
exporter.export()
```

## Available Properties

| Property | Description | Default |
|----------|-------------|---------|
| `model` | Django model to export (required) | `None` |
| `fields` | List of field names to export (required) | `[]` |
| `batch` | Number of records to process in each batch | `1000` |
| `table_name` | Full BigQuery table name (required) | `''` |
| `replace_nulls_with_empty` | Whether to replace `None` values with empty strings | `False` |

## Available Methods

### define_queryset()

Define the queryset to export. Override this method to filter or order your data.

```python
def define_queryset(self):
    # Only export books published in the last year
    one_year_ago = datetime.date.today() - datetime.timedelta(days=365)
    return self.model.objects.filter(publication_date__gte=one_year_ago).order_by('id')
```

### export(pull_date=None)

Export data to BigQuery. The `pull_date` parameter can be used to record when the data was exported.

```python
exporter = BookExporter()
errors = exporter.export()
if errors:
    print(f"Encountered {len(errors)} errors during export")
```

### table_has_data(pull_date=None)

Check if the BigQuery table already has data for a specific pull date.

```python
exporter = BookExporter()
pull_date = datetime.datetime.now()
if not exporter.table_has_data(pull_date):
    exporter.export(pull_date=pull_date)
else:
    print("Data already exported for today")
```

## Custom Fields

Use the `@custom_field` decorator to create methods that transform data during export:

```python
@custom_field
def full_name(self, instance):
    return f"{instance.first_name} {instance.last_name}"

@custom_field
def category_details(self, instance):
    # Return complex nested data
    return {
        'id': instance.category_id,
        'name': instance.category.name,
        'parent': instance.category.parent.name if instance.category.parent else None
    }
```

## Complete Example

Here's a complete example with a Book model:

```python
import datetime
from bigquery_exporter.base import BigQueryExporter, custom_field
from myapp.models import Book

class BookExporter(BigQueryExporter):
    model = Book
    batch = 1000
    table_name = 'my_project.bookstore.books'
    fields = [
        'id', 'title', 'author', 'publication_date', 'is_bestseller',
        'genre', 'page_count', 'created_at', 'updated_at', 'rating'
    ]

    def define_queryset(self):
        # Only export books updated in the last 30 days
        thirty_days_ago = datetime.date.today() - datetime.timedelta(days=30)
        return Book.objects.filter(updated_at__gte=thirty_days_ago).order_by('id')

    @custom_field
    def genre(self, instance):
        """Return both the code and display name for the genre"""
        GENRES = {
            'SFF': 'Science Fiction & Fantasy',
            'MYS': 'Mystery',
            'ROM': 'Romance',
            # ... other genres
        }
        return {
            'code': instance.genre,
            'name': GENRES.get(instance.genre, 'Unknown')
        }

    @custom_field
    def rating(self, instance):
        """Calculate and return the average rating"""
        avg_rating = instance.reviews.aggregate(avg=Avg('rating'))['avg'] or 0
        return round(avg_rating, 1)

# In a task or management command
def export_books_to_bigquery():
    pull_date = datetime.datetime.now()

    exporter = BookExporter(
        project='my-gcp-project',
        credentials='/path/to/credentials.json'
    )

    # Check if data already exists for today
    if exporter.table_has_data(pull_date) and not force_export:
        print(f"Data already exists for {pull_date.date()}, skipping export")
        return

    # Perform the export
    errors = exporter.export(pull_date=pull_date)

    if errors:
        print(f"Export completed with {len(errors)} errors")
    else:
        print(f"Successfully exported books to BigQuery")
```

## Error Handling

The `export()` method returns a list of error objects for any failed row insertions. Each error includes:
- The row index
- The error message
- The affected data

You can use this information to log errors or retry specific records.

## Testing with Dependency Injection

The BigQueryExporter supports dependency injection for easier testing without relying on actual Google API connections:

```python
from unittest.mock import MagicMock
from bigquery_exporter.base import BigQueryExporter

# Create a mock client
mock_client = MagicMock()
mock_table = MagicMock()
mock_table.schema = [MagicMock(name='id'), MagicMock(name='title')]
mock_client.get_table.return_value = mock_table
mock_client.insert_rows.return_value = []  # No errors

# Test your exporter with the mock client
class BookExporter(BigQueryExporter):
    model = Book
    fields = ['id', 'title']
    table_name = 'project.dataset.books'

# Use dependency injection to provide the mock client
exporter = BookExporter(client=mock_client)

# Now you can test without actual API connections
results = exporter.export()
```

You can also inject a custom client factory for more control:

```python
class TestClientFactory:
    @staticmethod
    def create_client(project=None, credentials=None):
        # Return a pre-configured mock client
        mock_client = MagicMock()
        # ... configure mock ...
        return mock_client

# Use the custom factory
exporter = BookExporter(client_factory=TestClientFactory)
```

These approaches make your tests:
- Faster by avoiding network calls
- More reliable by removing external dependencies
- Easier to run in CI/CD environments

## Best Practices

1. Always define an ordering in `define_queryset()` when using batching
2. Set appropriate batch sizes based on your model's complexity
3. Use custom fields to preprocess data before export
4. Implement idempotency checks with `table_has_data()`
5. Catch and handle `GoogleAPICallError` and `BigQueryExporterError` exceptions
6. Use dependency injection in tests to avoid actual API connections

## License

This project is licensed under the MIT License - see the LICENSE file for details.