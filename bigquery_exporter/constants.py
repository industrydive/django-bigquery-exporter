"""
Constants for the BigQuery Exporter package.
"""

# Default values to use for each BigQuery field type when replacing None values
BQ_TYPE_DEFAULTS = {
    'STRING': '',
    'INTEGER': 0,
    'FLOAT': 0.0,
    'NUMERIC': 0.0,
    'BOOLEAN': False,
    'TIMESTAMP': '',
    'DATE': '',
    'TIME': '',
    'DATETIME': '',
    'RECORD': {},
    'BYTES': b'',
    'JSON': '{}'
}