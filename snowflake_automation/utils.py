import snowflake.connector
import os

def get_connection():
    """
    Establishes a connection to Snowflake using environment variables.
    """
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    role = os.getenv("SNOWFLAKE_ROLE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    if not all([user, password, account]):
        print("Environment variables not set. Please enter credentials:")
        account = account or input("Snowflake Account (e.g. xy12345.us-east-1): ")
        user = user or input("Snowflake User: ")
        import getpass
        password = password or getpass.getpass("Snowflake Password: ")
        warehouse = warehouse or input("Warehouse (default COMPUTE_WH): ") or "COMPUTE_WH"
        role = role or input("Role (default ACCOUNTADMIN): ") or "ACCOUNTADMIN"
        database = database or input("Database (optional): ")
        schema = schema or input("Schema (optional): ")

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema
    )
    return conn

def execute_sql_file(conn, file_path):
    """
    Reads a SQL file and executes the commands using execute_string.
    This handles comments and multiple statements correctly.
    """
    print(f"Executing {file_path}...")
    with open(file_path, 'r') as f:
        sql_content = f.read()

    try:
        # execute_string returns a list of cursors, one for each statement executed
        cursors = conn.execute_string(sql_content)
        for cursor in cursors:
            # We can iterate over results if needed, but for DDL/DML usually just checking success is enough
            print(f"Success: Statement executed (Query ID: {cursor.sfqid})")
    except Exception as e:
        print(f"Error executing file {file_path}:")
        print(f"Error details: {e}")

