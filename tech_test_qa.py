import re
import psycopg2

class DataQualityChecker:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    # ====================================== TEST 1: CHECK UNEXOECTED SYMBOLS IN DATA ======================================
    # Check if there are any unexpected SYMBOLS 

    def check_unexpected_symbols(self, table_name):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name} WHERE symbol !~ '[A-Za-z0-9]+'"
        cursor.execute(query)
        unexpected_symbols = cursor.fetchall()
        cursor.close()
        conn.close()
        if unexpected_symbols:
            print("Unexpected symbols found:")
            print(unexpected_symbols)


    # ====================================== TEST 2: CHECK VALUES BASED IN GIVEN INFO ON DATA ======================================
    # Check if there are any unexpected Values 

    def check_unexpected_values(self, table_name):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name} WHERE digits < 0 OR cmd NOT IN (0, 1) OR volume < 0 OR contractsize < 0"
        cursor.execute(query)
        unexpected_values = cursor.fetchall()
        cursor.close()
        conn.close()
        if unexpected_values:
            print("Unexpected numerical values found:")
            print(unexpected_values)
    # ====================================== TEST 3: CHECK Date Formats ======================================
    # Check if there are any unexpected Dates 

    def check_unexpected_dates(self, table_name):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name} WHERE open_time > close_time"
        cursor.execute(query)
        unexpected_dates = cursor.fetchall()
        cursor.close()
        conn.close()
        if unexpected_dates:
            print("Unexpected dates found:")
            print(unexpected_dates)


    # ====================================== TEST 4: CHECK UNMATCH TRAGES(JOINS) ======================================
    # Check if there are any NULL values generated After Join 

    def check_unmatched_trades(self, table_name):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table_name} t LEFT JOIN users u ON t.login_hash = u.login_hash AND u.server_hash=t.server_hash WHERE u.login_hash IS NULL or u.server_hash IS NULL"
        cursor.execute(query)
        unmatched_trades = cursor.fetchone()
        cursor.close()
        conn.close()
        if unmatched_trades[0] > 0:
            print("Unmatched trades found:")
            print(unmatched_trades)


    # ====================================== TEST 5: EMPTY VALUES CHECK ======================================
    # Check if there are any empty values present in the table 
  
    def check_empty_values_in_table(self, schema_name, table_name):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        query = f"SELECT * FROM {schema_name}.{table_name}"
        cursor.execute(query)
        sql_results = cursor.fetchall()

        row_no = 0
        for record in sql_results:
            row_no += 1
            for cell_value in record:
                if cell_value is None:
                    print(f"There is an empty value in the '{schema_name}.{table_name}' table on row '{row_no}'.")

   # ====================================== TEST 6: NULL VALUES CHECK ======================================
   # Check if there are any NULL values present in the table 

    def check_null_values_in_table(self, schema_name, table_name):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'")
        columns = cursor.fetchall()

        for column in columns:
            sql_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {column[0]} IS NULL"
            cursor.execute(sql_query)
            sql_result = cursor.fetchone()

            if sql_result[0] > 0:
                print(f"The {column} column has NULL values.")

    # ====================================== TEST 7: DATE FORMATTING CHECK ======================================
    # Check the date columns contain values in the 'yyyy-mm-dd' format 

    def check_date_formatting_constraint(self, schema_name, table_name):
        expected_date_format = r"^\d{4}-\d{2}-\d{2}$"
        data_type = 'timestamp without time zone'

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND data_type = '{data_type}'")
        sql_results_1 = cursor.fetchall()
        date_columns = [sql_result[0] for sql_result in sql_results_1]

        for date_column in date_columns:
            query = f"SELECT {date_column} FROM {schema_name}.{table_name}"
            cursor.execute(query)
            sql_results_2 = cursor.fetchall()
            for sql_result in sql_results_2:
                date_value = sql_result[0].strftime("%Y-%m-%d")
                if not re.match(expected_date_format, date_value):
                    print("Invalid date detected - date values should be in 'yyyy-mm-dd' format.")


    # ====================================== TEST 8: ID CHARACTER LENGTH CONSTRAINT CHECK ======================================
  
    # Test all the ID columns in the table contain 32 characters in length  assumtion(length of id is 32) 

    def check_id_char_length_constraint(self, schema_name, table_name):
        expected_id_char_length = 32

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name LIKE '%_hash'")
        sql_results = cursor.fetchall()

        for sql_result in sql_results:
            id_column = sql_result[0]
            actual_id_length = len(id_column)
            if actual_id_length != expected_id_char_length:
                print(f"Invalid ID column found: All ID columns must be {expected_id_char_length} characters long but it has {actual_id_length} in some rows. The ID column containing invalid IDs is '{id_column}' column.")


    # ====================================== TEST 9: DUPLICATES CHECK ======================================
    
   # Test the number of duplicate records

    def check_duplicate_records_count(self, schema_name, table_name):
        column_name = "login_hash"

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()
        query = f"SELECT {column_name}, COUNT(*) FROM {schema_name}.{table_name} GROUP BY {column_name} HAVING COUNT(*) > 1"
        cursor.execute(query)

        duplicates = cursor.fetchall()
        total_no_of_duplicates = len(duplicates)

        if total_no_of_duplicates > 0:
            print(f"Duplicate entries detected - {table_name} contains {total_no_of_duplicates} duplicate entries.")

    def check_data_quality(self, schema_name, table_name):
        self.check_unexpected_symbols(table_name)
        self.check_unexpected_values(table_name)
        self.check_unexpected_dates(table_name)
        self.check_unmatched_trades(table_name)
        self.check_empty_values_in_table(schema_name, table_name)
        self.check_null_values_in_table(schema_name, table_name)
        self.check_date_formatting_constraint(schema_name, table_name)
        self.check_id_char_length_constraint(schema_name, table_name)
        self.check_duplicate_records_count(schema_name, table_name)


# Usage
checker = DataQualityChecker(
    host='hostname',
    port=5432,
    database="DB NAME",
    user="USER name",
    password="PASSWORD"
)

schema_name = "public"
table_name = "trades"

checker.check_data_quality(schema_name, table_name)
