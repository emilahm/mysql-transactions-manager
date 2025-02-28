import argparse
import csv
import logging
import time
from datetime import datetime
from decimal import Decimal
import mysql.connector
from mysql.connector import errorcode
from transactions_sql import sql_commands


def get_logger():
    formatter = logging.Formatter(
        fmt="%(asctime)s - [%(levelname)-5s] - [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger


logger = get_logger()


def parse_args():
    parser = argparse.ArgumentParser(description="Script to manage a transactions database, upload data, and query it.")

    # Common DB arguments - shared
    parser.add_argument("--db-user", default="root", help="MySQL username.")
    parser.add_argument("--db-password", default="", help="MySQL user password.")
    parser.add_argument("--db-host", default="127.0.0.1", help="MySQL host.")
    parser.add_argument("--db-port", type=int, default=3306, help="MySQL port.")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # ---------------------
    # setup sub-command
    # ---------------------
    setup_parser = subparsers.add_parser("setup", help="Create the database (if needed) and create tables.")
    setup_parser.add_argument("--db-name",
                              default="transactions",
                              help="Name of the database to create/use. Default: transactions")

    # ---------------------
    # upload sub-command
    # ---------------------
    upload_parser = subparsers.add_parser("upload", help="Upload CSV data into the database.")
    upload_parser.add_argument("--db-name",
                               default="transactions",
                               help="Name of the database to create/use. Default: transactions")
    upload_parser.add_argument("--csv-file",
                               default="./transactions.csv",
                               help="Path to the transactions CSV file. Default: ./transactions.csv")

    # ---------------------
    # query sub-command
    # ---------------------
    query_parser = subparsers.add_parser("query", help="Query the database.")
    query_parser.add_argument("--db-name",
                              default="transactions",
                              help="Name of the database to query. Default: transactions")
    query_parser.add_argument("--query-name",
                              default="get_customers",
                              help="Query name for the database. Default: get_customers")
    query_parser.add_argument("--store-name",
                              default="King St",
                              help="Store name to filter on. Default: King St")
    query_parser.add_argument("--product-name",
                              default="cappuccino",
                              help="Product name to filter on. Default: cappuccino")

    return parser.parse_args()


def connect_to_mysql(config, attempts=3, delay=2):
    logger.info("connect_to_mysql.start: (user=%s, host=%s, port=%s)",
                config['user'], config['host'], config['port'])
    attempt = 1
    while attempt <= attempts:
        try:
            connection = mysql.connector.connect(**config)
            logger.info("connect_to_mysql.end: (user=%s, host=%s, port=%s)",
                        config['user'], config['host'], config['port'])
            return connection
        except mysql.connector.Error as err:
            logger.error("connect_to_mysql.error: (attempt %d/%d, user=%s) => %s",
                         attempt, attempts, config['user'], err)
            if attempt == attempts:
                return None

            time.sleep(delay ** attempt)
            attempt += 1
    return None


def create_database(cursor, db_name, db_user):
    logger.info("create_database.start: (user=%s, db=%s)", db_user, db_name)
    try:
        cursor.execute(sql_commands["create_database"].format(db_name))
    except mysql.connector.Error as err:
        logger.error("create_database.error: (user=%s) failed to create database ('%s') => %s",
                     db_user, db_name, err)
    logger.info("create_database.end: (user=%s, db=%s)", db_user, db_name)


def use_database(cursor, db_name, db_user):
    logger.info("use_database.start: (user=%s, db=%s)", db_user, db_name)
    try:
        cursor.execute(sql_commands["use_database"].format(db_name))
        logger.info("use_database: (user=%s) now using database '%s'", db_user, db_name)
    except mysql.connector.Error as err:
        logger.error("use_database.error: (user=%s, db=%s) => %s",
                     db_user, db_name, err)
    logger.info("use_database.end: (user=%s, db=%s)", db_user, db_name)


def create_tables(cursor, db_name, db_user):
    logger.info("create_tables.start: (user=%s, db=%s)", db_user, db_name)
    for key, command in sql_commands.items():
        if not key.startswith("table"):
            continue
        try:
            logger.info("create_tables: (user=%s, db=%s) creating table '%s'", db_user, db_name, key)
            cursor.execute(command)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logger.info("create_tables: (user=%s, db=%s) table '%s' already exists", db_user, db_name, key)
            else:
                logger.error("create_tables.error: (user=%s, db=%s) creating '%s' => %s", db_user, db_name, key, err)
    logger.info("create_tables.end: (user=%s, db=%s)", db_user, db_name)


def row_to_transaction(row):
    return (
        row['transaction_id'].strip(),
        datetime.strptime(row['transaction_date'].strip(), '%Y-%m-%d').date(),
        row['product_name'].strip(),
        Decimal(row['price'].strip()),
        row['store_name'].strip(),
        row['sales_representative_name'].strip(),
        row['client_name'].strip(),
    )


def insert_temp_data(csv_file, cursor, cnx, db_name, db_user):
    logger.info("insert_temp_data.start: (user=%s, db=%s, file=%s)", db_user, db_name, csv_file)
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                transaction = row_to_transaction(row)
                try:
                    logger.debug("insert_temp_data: (user=%s, db=%s) inserting row with id '%s'", db_user, db_name,
                                 row['transaction_id'])
                    cursor.execute(sql_commands["insert_transactions_temp"], transaction)
                except mysql.connector.Error as err:
                    logger.error("insert_temp_data.error: (user=%s, db=%s) failed to insert %s => %s", db_user, db_name,
                                 transaction, err)

        cnx.commit()
        logger.info("insert_temp_data.end: (user=%s, db=%s)", db_user, db_name)
    except Exception as e:
        logger.error("insert_temp_data.error: (user=%s, db=%s) reading CSV file '%s' => %s", db_user, db_name, csv_file,
                     e)


def fix_temp_data(cursor, cnx, db_name, db_user):
    logger.info("fix_temp_data.start: (user=%s, db=%s)", db_user, db_name)
    try:
        cursor.execute(sql_commands['update_transactions_temp'])
        cnx.commit()
        logger.info("fix_temp_data.end: (user=%s, db=%s)", db_user, db_name)
    except mysql.connector.Error as err:
        logger.error("fix_temp_data.error: updating temp data (user=%s, db=%s) => %s", db_user, db_name, err)


def insert_data(cursor, cnx, db_user, db_name):
    logger.info("insert_data.start: (user=%s, db=%s)", db_user, db_name)
    for key, command in sql_commands.items():
        if not key.startswith("insert") or key.endswith("temp"):
            continue
        logger.info("insert_data: (user=%s, db=%s) running '%s'", db_user, db_name, key)
        try:
            cursor.execute(command)
            cnx.commit()
        except mysql.connector.Error as err:
            logger.error("insert_data.error: (user=%s, db=%s) in '%s' => %s", db_user, db_name, key, err)
    logger.info("insert_data.end: (user=%s, db=%s)", db_user, db_name)


def run_query(cursor, db_name, db_user, store_name, product_name, query_key="get_customers_sort_optim"):
    logger.info("run_query.start: (user=%s, db=%s, query_key=%s, store_name=%s, product_name=%s)",
                db_user, db_name, query_key, store_name, product_name)
    if query_key not in sql_commands:
        logger.error("run_query.error: (user=%s, db=%s, query_key=%s) query key not found", db_user,
                     db_name, query_key)
        return []

    sql = sql_commands[query_key].format(store_name=store_name, product_name=product_name)
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.info("run_query.end: (user=%s, db=%s, query_key=%s) returned %d rows", db_user,
                    db_name, query_key, len(results))
        return results
    except mysql.connector.Error as err:
        logger.error("run_query.error: (user=%s, db=%s, query_key=%s) => %s", db_user,
                     db_name, query_key, err)
        return []


def print_table(data):
    headers = ["ID", "Name", "Date", "Total Amount"]
    if not data:
        return
    column_widths = []

    num_columns = len(data[0])

    combined = [tuple(headers)] + data
    for i in range(num_columns):
        column_widths.append(max(len(str(row[i])) for row in combined))

    header_row = " | ".join(f"{headers[i]:<{column_widths[i]}}" for i in range(num_columns))
    print("-" * len(header_row))
    print(header_row)
    print("-" * len(header_row))

    for row in data:
        print(" | ".join(f"{str(row[i]):<{column_widths[i]}}" for i in range(len(row))))
    print("-" * len(header_row))


def main():
    args = parse_args()

    db_config = {
        'user': args.db_user,
        'password': args.db_password,
        'host': args.db_host,
        'port': args.db_port,
        'database': '',
        'raise_on_warnings': True
    }

    cnx = connect_to_mysql(db_config)
    if not cnx:
        logger.error("main: could not connect to MySQL, exiting.")
        return

    cursor = cnx.cursor()

    # -------------------------------------
    # Handle sub-commands
    # -------------------------------------
    if args.command == "setup":
        db_name = args.db_name

        create_database(cursor, db_name, args.db_user)
        use_database(cursor, db_name, args.db_user)
        create_tables(cursor, db_name, args.db_user)

        logger.info("setup: database '%s' and tables created or verified.", db_name)

    elif args.command == "upload":
        db_name = args.db_name

        use_database(cursor, db_name, args.db_user)

        insert_temp_data(args.csv_file, cursor, cnx, db_name, args.db_user)
        fix_temp_data(cursor, cnx, db_name, args.db_user)
        insert_data(cursor, cnx, db_name, args.db_user)

        logger.info("upload: data uploaded from '%s' into DB '%s'.", args.csv_file, db_name)

    elif args.command == "query":
        db_name = args.db_name
        use_database(cursor, db_name, args.db_user)
        rows = run_query(cursor, db_name, args.db_user,
                         store_name=args.store_name,
                         product_name=args.product_name,
                         query_key=args.query_name)
        print(f"Results from {args.query_name}:")
        print_table(rows)

    cursor.close()
    cnx.close()
    logger.info("main: completed.")


# -----------------------------------------
# CLI Entry
# -----------------------------------------
if __name__ == "__main__":
    main()
