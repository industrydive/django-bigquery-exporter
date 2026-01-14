"""
Google Cloud BigQuery client implementation
"""
import logging
from typing import Any, Dict, List, Optional, Union

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.retry import Retry

from bigquery_exporter.clients.interface import BigQueryClientInterface
from bigquery_exporter.errors import BigQueryExporterInitError


logger = logging.getLogger(__name__)


class GoogleBigQueryClient(BigQueryClientInterface):
    """
    Implementation of BigQueryClientInterface that uses the Google Cloud BigQuery client.
    """

    def __init__(
            self,
            project: Optional[str] = None,
            credentials: Optional[Union[str, service_account.Credentials]] = None,
        ):
        """
        Initialize a GoogleBigQueryClient.

        Args:
            project: The Google Cloud project ID.
            credentials: Credentials to use for authentication. Can be either:
                         - str: Path to a service account JSON key file
                         - google.oauth2.service_account.Credentials: An existing
                           service account credentials object
                         If None, falls back to Application Default Credentials (ADC).

        Raises:
            BigQueryExporterInitError: If an error occurs while initializing the client.
            ValueError: If credentials is provided but has an unsupported type.
        """
        try:
            # Application Default Credentials (ADC)
            if credentials is None:
                self.client = bigquery.Client(project=project)
            # Path to service account JSON key file
            elif isinstance(credentials, str):
                credentials_obj = service_account.Credentials.from_service_account_file(credentials)
                self.client = bigquery.Client(project=project, credentials=credentials_obj)
            # Existing service account credentials object            
            elif isinstance(credentials, service_account.Credentials):
                self.client = bigquery.Client(project=project, credentials=credentials)
            else:
                raise ValueError(f'Unsupported credentials type: "{type(credentials).__name__}". Expected str or service_account.Credentials.')
        except GoogleAPICallError as e:
            logger.error(f"Error creating BigQuery client: {e}")
            raise BigQueryExporterInitError(e)

    def get_table(self, table_id: str) -> Any:
        """
        Gets a table from BigQuery.

        Args:
            table_id: The ID of the table to get.

        Returns:
            The BigQuery table.
        """
        return self.client.get_table(table_id)

    def insert_rows(self, table: Any, rows: List[Dict[str, Any]],
                   retry: Optional[Retry] = None) -> List[Dict[str, Any]]:
        """
        Inserts rows into a BigQuery table.

        Args:
            table: The table to insert rows into.
            rows: The rows to insert.
            retry: A retry object used for retrying requests.

        Returns:
            A list of errors that occurred while inserting rows, empty if no errors.
        """
        return self.client.insert_rows(table, rows, retry=retry)

    def query(self, query: str) -> Any:
        """
        Executes a query in BigQuery.

        Args:
            query: The query to execute.

        Returns:
            A query job.
        """
        return self.client.query(query)