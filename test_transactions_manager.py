import unittest
from unittest.mock import patch, MagicMock, mock_open
from datetime import date
from decimal import Decimal

from transactions_manager import (
    row_to_transaction,
    insert_temp_data,
    fix_temp_data,
    insert_data,
    run_query,
)


class TestTransactionsManager(unittest.TestCase):
    def test_row_to_transaction(self):
        """
        Test that row_to_transaction correctly converts a CSV row (dict) into the tuple
        expected by the 'insert_transactions_temp' SQL statement.
        """
        sample_row = {
            'transaction_id': ' 123 ',
            'transaction_date': '2023-01-15',
            'product_name': ' cappuccino ',
            'price': ' 4.50 ',
            'store_name': ' King St ',
            'sales_representative_name': ' Alice ',
            'client_name': ' Bob '
        }

        result = row_to_transaction(sample_row)
        expected = (
            '123',
            date(2023, 1, 15),
            'cappuccino',
            Decimal('4.50'),
            'King St',
            'Alice',
            'Bob'
        )
        self.assertEqual(result, expected)

    @patch("transactions_manager.open", new_callable=mock_open, read_data=
    "transaction_id,transaction_date,product_name,price,store_name,sales_representative_name,client_name\n"
    "1,2023-01-10,latte,3.50,Queen St,John Doe,Customer A\n"
    "2,2023-01-11,espresso,2.75,King St,Jane Doe,Customer B\n"
           )
    def test_insert_temp_data(self, mock_file):
        """
        Test that insert_temp_data reads the CSV file and inserts each row into the temp table.
        We'll mock the file I/O and cursor to verify the expected inserts were called.
        """
        mock_cursor = MagicMock()
        mock_cnx = MagicMock()

        insert_temp_data(
            csv_file="fake_path.csv",
            cursor=mock_cursor,
            cnx=mock_cnx,
            db_name="test_db",
            db_user="test_user"
        )

        mock_file.assert_called_once_with("fake_path.csv", 'r', newline='', encoding='utf-8')

        self.assertEqual(mock_cursor.execute.call_count, 2, "Should insert 2 rows")

        expected_first = (
            'insert_transactions_temp',
            (
                '1',
                date(2023, 1, 10),
                'latte',
                Decimal('3.50'),
                'Queen St',
                'John Doe',
                'Customer A',
            )
        )
        expected_second = (
            'insert_transactions_temp',
            (
                '2',
                date(2023, 1, 11),
                'espresso',
                Decimal('2.75'),
                'King St',
                'Jane Doe',
                'Customer B',
            )
        )


        all_calls = mock_cursor.execute.call_args_list

        first_insert_params = all_calls[0][0][1]
        second_insert_params = all_calls[1][0][1]

        self.assertEqual(first_insert_params, expected_first[1])
        self.assertEqual(second_insert_params, expected_second[1])

        mock_cnx.commit.assert_called_once()

    def test_fix_temp_data(self):
        """
        Test that fix_temp_data executes the expected update on the temp table.
        """
        mock_cursor = MagicMock()
        mock_cnx = MagicMock()

        fix_temp_data(cursor=mock_cursor, cnx=mock_cnx, db_name="test_db", db_user="test_user")

        mock_cursor.execute.assert_called_once()
        mock_cnx.commit.assert_called_once()

    def test_insert_data(self):
        """
        Test that insert_data loops over and executes all insert statements (except temp).
        """
        mock_cursor = MagicMock()
        mock_cnx = MagicMock()


        with patch("transactions_manager.sql_commands", {
            "insert_transactions_temp": "INSERT INTO temp_table ...",
            "insert_some_table": "INSERT INTO some_table ...",
            "insert_another_table": "INSERT INTO another_table ...",
            "table_something": "CREATE TABLE something(...)"
        }):
            insert_data(cursor=mock_cursor, cnx=mock_cnx, db_user="test_user", db_name="test_db")

        self.assertEqual(mock_cursor.execute.call_count, 2)

        self.assertEqual(mock_cnx.commit.call_count, 2)

    def test_run_query(self):
        """
        Test that run_query calls cursor.execute() with the properly formatted SQL
        and returns the fetched rows.
        """
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "Customer A", "2023-01-10", Decimal("3.50")),
            (2, "Customer B", "2023-01-11", Decimal("2.75")),
        ]

        with patch("transactions_manager.sql_commands", {
            "get_customers": "SELECT id, name, date, amount FROM transactions "
                             "WHERE store_name='{store_name}' AND product_name='{product_name}'"
        }):
            rows = run_query(
                cursor=mock_cursor,
                db_name="test_db",
                db_user="test_user",
                store_name="King St",
                product_name="espresso",
                query_key="get_customers"
            )

        mock_cursor.execute.assert_called_once_with(
            "SELECT id, name, date, amount FROM transactions "
            "WHERE store_name='King St' AND product_name='espresso'"
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], (1, "Customer A", "2023-01-10", Decimal("3.50")))


if __name__ == "__main__":
    unittest.main()