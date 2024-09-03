import pandas as pd
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent
import mysql.connector as m_connect
import logging
import json
import os

# Configure logging to display info level messages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLConnector:
    """
    A class to manage MySQL database connections and execute queries.
    """
    def __init__(self, host, port, user, password, database=None):
        """
        Initialize the MySQLConnector with connection parameters.

        :param host: Host address of the MySQL server.
        :param port: Port number of the MySQL server.
        :param user: Username for connecting to the MySQL server.
        :param password: Password for the specified username.
        :param database: Optional database name to connect to.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def get_connection(self):
        """
        Establish and return a connection to the MySQL database.

        :return: MySQL connection object.
        """
        return m_connect.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def execute_query(self, query, values=None):
        """
        Execute a SQL query with optional values.

        :param query: SQL query string to execute.
        :param values: Optional parameters for the SQL query.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        if values:
            cursor.executemany(query, values)
        else:
            cursor.execute(query)
        conn.commit()  # Commit changes to the database
        cursor.close()
        conn.close()

class PositionManager:
    """
    A class to manage the binlog position for resuming binlog processing.
    """
    def __init__(self, position_file):
        """
        Initialize the PositionManager with the file path to store the binlog position.

        :param position_file: Path to the file that stores the last processed binlog position.
        """
        self.position_file = position_file

    def load_last_position(self):
        """
        Load the last binlog position from a file.

        :return: Dictionary with 'file' and 'pos' if file exists, otherwise None.
        """
        if os.path.exists(self.position_file):
            with open(self.position_file, 'r') as f:
                return json.load(f)
        return None

    def save_last_position(self, position):
        """
        Save the current binlog position to a file.

        :param position: Dictionary with 'file' and 'pos' to be saved.
        """
        with open(self.position_file, 'w') as f:
            json.dump(position, f)

class BinlogProcessor:
    """
    A class to process MySQL binlog events and apply changes to another table.
    """
    def __init__(self, mysql_settings, position_file):
        """
        Initialize the BinlogProcessor with MySQL settings and position file.

        :param mysql_settings: Dictionary containing MySQL connection settings.
        :param position_file: Path to the file that stores the last processed binlog position.
        """
        self.mysql_settings = mysql_settings
        self.position_manager = PositionManager(position_file)
        self.mysql_connector = MySQLConnector(
            host=self.mysql_settings['host'],
            port=self.mysql_settings['port'],
            user=self.mysql_settings['user'],
            password=self.mysql_settings['password'],
            database='inventory'  # Database to apply changes to
        )
        self.stream = None

    def start_stream(self):
        """
        Start the binlog stream reader from the last saved position.
        """
        last_position = self.position_manager.load_last_position()
        self.stream = BinLogStreamReader(
            connection_settings=self.mysql_settings,
            server_id=223344,  # Unique server ID for the replication stream
            resume_stream=True,
            blocking=True,
            only_events=[UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent],
            log_pos=last_position.get('pos') if last_position else None,
            log_file=last_position.get('file') if last_position else None
        )

    def process_events(self):
        """
        Process incoming binlog events and apply changes to the target tables.
        """
        for binlog_event in self.stream:
            event_type = type(binlog_event).__name__
            table_name = binlog_event.table
            rows = [row for row in binlog_event.rows]
            columns = [col.name for col in binlog_event.columns]

            # Handle events for specific tables
            if table_name == 'table1':
                self.handle_table1_event(event_type, columns, rows)
            elif table_name == 'table2':
                self.handle_table2_event(event_type, columns, rows)
            else:
                logger.info(f"Unhandled table: {table_name}")

            # Save the current binlog position for resuming later
            last_position = {
                'file': self.stream.log_file,
                'pos': self.stream.log_pos
            }
            self.position_manager.save_last_position(last_position)

    def handle_table1_event(self, event_type, columns, rows):
        """
        Handle binlog events for 'table1' and apply changes to 'table3'.

        :param event_type: Type of binlog event (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent).
        :param columns: List of column names in the table.
        :param rows: List of row data affected by the event.
        """
        if event_type == 'WriteRowsEvent':
            self.handle_write_rows_event(columns, rows)
        elif event_type == 'UpdateRowsEvent':
            self.handle_update_rows_event(columns, rows)
        elif event_type == 'DeleteRowsEvent':
            self.handle_delete_rows_event(rows)

    def handle_write_rows_event(self, columns, rows):
        """
        Handle WriteRowsEvent for 'table1' and insert rows into 'table3'.

        :param columns: List of column names in the table.
        :param rows: List of new rows added to 'table1'.
        """
        placeholders = ', '.join(columns)
        insert_values_list = [entry['values'] for entry in rows]
        latest_insert_df = pd.DataFrame(insert_values_list)
        sql = f"INSERT INTO table3 ({placeholders}) VALUES (%s, %s, %s, %s);"
        values = list(latest_insert_df.itertuples(index=False, name=None))
        self.mysql_connector.execute_query(sql, values)
        logger.info(f"Inserted rows into table3: {latest_insert_df}")

    def handle_update_rows_event(self, columns, rows):
        """
        Handle UpdateRowsEvent for 'table1' and update rows in 'table3'.

        :param columns: List of column names in the table.
        :param rows: List of updated rows in 'table1'.
        """
        placeholders = ', '.join(columns)
        update_values_list = [entry['after_values'] for entry in rows]
        latest_update_df = pd.DataFrame(update_values_list)
        sql = (
            f"INSERT INTO table3 ({placeholders}) VALUES (%s, %s, %s, %s) "
            f"ON DUPLICATE KEY UPDATE orderItem = VALUES(orderItem), "
            f"customerName = VALUES(customerName), orderDate = VALUES(orderDate);"
        )
        values = list(latest_update_df.itertuples(index=False, name=None))
        self.mysql_connector.execute_query(sql, values)
        logger.info(f"Updated rows in table3: {latest_update_df}")

    def handle_delete_rows_event(self, rows):
        """
        Handle DeleteRowsEvent for 'table1' and delete rows from 'table3'.

        :param rows: List of deleted rows from 'table1'.
        """
        delete_values_list = [entry['values'] for entry in rows]
        latest_delete_df = pd.DataFrame(delete_values_list)
        order_ids = latest_delete_df['orderId'].tolist()
        placeholders = ', '.join(map(str, order_ids))
        sql = f"DELETE FROM table3 WHERE orderId IN ({placeholders})"
        self.mysql_connector.execute_query(sql)
        logger.info(f"Deleted rows from table3: {latest_delete_df}")

    def handle_table2_event(self, event_type, columns, rows):
        """
        Handle binlog events for 'table2' and update 'table3' accordingly.

        :param event_type: Type of binlog event (WriteRowsEvent).
        :param columns: List of column names in the table.
        :param rows: List of row data affected by the event.
        """
        if event_type == 'WriteRowsEvent':
            self.handle_table2_write_rows_event(columns, rows)

    def handle_table2_write_rows_event(self, columns, rows):
        """
        Handle WriteRowsEvent for 'table2' and update order status in 'table3'.

        :param columns: List of column names in the table.
        :param rows: List of new rows added to 'table2'.
        """
        latest_insert_df = pd.DataFrame([entry['values'] for entry in rows])
        for _, row in latest_insert_df.iterrows():
            self.update_order_status(row)

    def update_order_status(self, row):
        """
        Update the order status in 'table3' based on data from 'table2'.

        :param row: DataFrame row with order status and ID.
        """
        status_map = {
            1: 'ordered',
            2: 'Processing',
            3: 'shipped',
            4: 'outForDelivery',
            5: 'delivered'
        }
        order_status = status_map.get(row['orderStatus'])
        if order_status:
            sql = f"UPDATE table3 SET orderStatus{row['orderStatus']} = '{order_status}' WHERE orderId = {row['orderId']}"
            self.mysql_connector.execute_query(sql)
            logger.info(f"Updated order status for orderId {row['orderId']} to {order_status}")

def main():
    """
    Main function to initialize and start the binlog processor.
    """
    mysql_settings = {
        "host": '127.0.0.1',
        "port": 3306,
        "user": 'root',
        "password": 'debezium'
    }
    position_file = 'last_position.json'

    processor = BinlogProcessor(mysql_settings, position_file)
    try:
        processor.start_stream()
        processor.process_events()
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if processor.stream:
            processor.stream.close()

if __name__ == "__main__":
    main()
