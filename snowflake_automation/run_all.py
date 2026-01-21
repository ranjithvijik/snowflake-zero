import os
import glob
from utils import get_connection, execute_sql_file

def main():
    print("Starting Zero to Snowflake Automation...")
    
    # 1. Connect to Snowflake
    try:
        conn = get_connection()
        print("Connected to Snowflake.")
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return

    # 2. Get all SQL scripts
    scripts_dir = os.path.join(os.path.dirname(__file__), 'scripts')
    # Sort by filename to ensure correct order (00, 01, 02...)
    sql_files = sorted(glob.glob(os.path.join(scripts_dir, '*.sql')))

    if not sql_files:
        print("No SQL scripts found in scripts directory.")
        return

    # 3. Execute scripts
    for sql_file in sql_files:
        filename = os.path.basename(sql_file)
        print(f"\n--- Running {filename} ---")
        try:
            execute_sql_file(conn, sql_file)
        except Exception as e:
            print(f"Failed to execute {filename}: {e}")
            # Decide if you want to abort or continue. 
            # For this guide, it's probably better to stop on error.
            # break 

    print("\nAutomation complete.")
    conn.close()

if __name__ == "__main__":
    main()
