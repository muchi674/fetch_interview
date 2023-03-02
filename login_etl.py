"""Login ETL Microservice"""
import datetime
import json
import typing

import boto3
import psycopg2
from psycopg2 import sql


class SQSInterface:
    """Handler to interact with aws SQS"""

    def __init__(self, region: str, endpoint_url: str, queue_url: str) -> None:
        """Instantiates a boto3 client to interact with the targeted aws SQS queue.

        Args:
            region (str): queue region
            endpoint_url (str): url to use with boto3 client
            queue_url (str): queue url
        """
        self.queue_url = queue_url
        self.client = boto3.client("sqs", region_name=region, endpoint_url=endpoint_url)

    def receive_message(self) -> typing.Any:
        """Receives one message from queue and returns its body as dictionary or None if aws didn't
        find any.

        Returns:
            typing.Any: see above
        """
        # reads 1 message
        response = self.client.receive_message(QueueUrl=self.queue_url)
        if "Messages" not in response or len(response["Messages"]) == 0:
            return None
        message = response["Messages"][0]
        self._delete_message(message["ReceiptHandle"])
        return json.loads(message["Body"])

    def _delete_message(self, receipt_handle):
        """Deletes successfully received message from queue to avoid repeated processing.

        As stated in its official documentation, aws SQS doesn't automatically delete a message
        after retrieving it for you, in case you don't successfully receive the message. This
        function is for internal use only thus the leading underscore.

        Args:
            receipt_handle: synonymous to message id
        """
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)


class PostgreSQLInterface:
    """Handler to interact with postgreSQL database"""

    def __init__(self, host: str, port: str, dbname: str, user: str, password: str) -> None:
        """Creates a new db session with the provided credentials

        Args:
            host (str): db host address
            port (str): connection port number
            dbname (str): database name
            dbuser (str): user to log in with
            password (str): user password
        """
        self.connection = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

    def write(self, table: str, row: dict) -> None:
        """Writes row to table

        Args:
            table (str): table to write to
            row (dict): column name to value pairs
        """
        # As advised by the official psycopg2 documentation, string interpolation and f string are
        # avoided below to prevent SQL injection attack
        table_name = sql.Identifier(table)
        column_names = sql.SQL(", ").join(sql.Identifier(col) for col in row.keys())
        placeholders = "(" + ", ".join(["%s"] * len(row)) + ")"
        with self.connection:
            with self.connection.cursor() as cursor:
                statement = sql.SQL("INSERT INTO {} ({}) VALUES " + placeholders + ";").format(
                    table_name, column_names
                )
                cursor.execute(statement, tuple(row.values()))


class LoginETLService:
    """Handler to extracting, transforming, and loading logins from SQS to PostgreSQL."""

    _expected_raw_login_fields = [
        "user_id",
        "device_type",
        "ip",
        "device_id",
        "locale",
        "app_version",
    ]

    def __init__(
        self,
        region: str,
        endpoint_url: str,
        queue_url: str,
        host: str,
        port: str,
        dbname: str,
        user: str,
        password: str,
        table: str,
    ) -> None:
        """Instantiates interfaces to services needed for login ETL.

        Args:
            region (str): queue region
            endpoint_url (str): url to use with boto3 client
            queue_url (str): queue url
            host (str): db host address
            port (str): connection port number
            dbname (str): database name
            dbuser (str): user to log in with
            password (str): user password
            table (str): table on specified db to write transformed logins to
        """

        self.table = table
        self.sqs_interface = SQSInterface(region, endpoint_url, queue_url)
        self.postgresql_interface = PostgreSQLInterface(host, port, dbname, user, password)

    def start(self):
        """Starts service."""

        while True:
            # extracts
            data = self.sqs_interface.receive_message()

            if not self._is_valid(data):
                continue

            # transforms
            data = self._transform(data)

            # load
            self.postgresql_interface.write(self.table, data)

    def _is_valid(self, data: dict) -> bool:
        """Returns True if raw login data contain all expected fields else False.

        Args:
            data (dict): raw login data

        Returns:
            bool: see above
        """
        return data is not None and all(field in data for field in self._expected_raw_login_fields)

    def _transform(self, data: dict) -> dict:
        """Masks PII data and add creation date before loading data into db.

        Args:
            data (dict): raw login data extracted from targeted queue

        Returns:
            dict: transformed login data
        """
        data["masked_ip"] = self._mask(data["ip"])
        data["masked_device_id"] = self._mask(data["device_id"])
        data["app_version"] = data["app_version"].replace(".", "")
        data["create_date"] = datetime.date.today()
        del data["ip"]
        del data["device_id"]
        return data

    def _mask(self, data: str) -> str:
        """Masks data in reversible way.

        Args:
            data (str): data to be masked

        Returns:
            str: masked data
        """
        mid = len(data) // 2
        return data[mid:] + data[:mid]


if __name__ == "__main__":

    # hardcoded parameters tailored for use with test environment
    login_etl_service = LoginETLService(
        region="us-east-1",
        endpoint_url="http://localhost:4566",
        queue_url="http://localhost:4566/000000000000/login-queue",
        host="localhost",
        port="5432",
        dbname="postgres",
        user="postgres",
        password="postgres",
        table="user_logins",
    )

    login_etl_service.start()
