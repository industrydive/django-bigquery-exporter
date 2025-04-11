"""
Google Cloud BigQuery client implementation
"""
import logging
from typing import Any, Dict, List, Optional

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

    def __init__(self, project: Optional[str] = None, credentials: Optional[str] = None):
        """
        Initialize a GoogleBigQueryClient.

        Args:
            project: The Google Cloud project ID.
            credentials: The path to the service account credentials file.

        Raises:
            BigQueryExporterInitError: If an error occurs while initializing the client.
        """
        try:
            if credentials:  # use service account credentials if provided
                service_account_info = service_account.Credentials.from_service_account_file(credentials)
                self.client = bigquery.Client(project=project, credentials=service_account_info)
            else:  # otherwise client will search application default credentials
                self.client = bigquery.Client(project=project)
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