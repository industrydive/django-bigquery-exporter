from google.api_core.exceptions import GoogleAPICallError


class BigQueryExporterError(Exception):
    pass


class BigQueryExporterInitError(BigQueryExporterError, GoogleAPICallError):
    pass


class BigQueryExporterValidationError(BigQueryExporterError):
    pass
