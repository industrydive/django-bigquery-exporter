"""
Interfaces for BigQuery clients
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from google.api_core.retry import Retry


class BigQueryClientInterface(ABC):
    """
    Interface for BigQuery clients.
    This defines the contract that all BigQuery client implementations must follow.
    """

    @abstractmethod
    def get_table(self, table_id: str) -> Any:
        """
        Gets a table from BigQuery.

        Args:
            table_id: The ID of the table to get.

        Returns:
            The BigQuery table.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def query(self, query: str) -> Any:
        """
        Executes a query in BigQuery.

        Args:
            query: The query to execute.

        Returns:
            A query job.
        """
        pass