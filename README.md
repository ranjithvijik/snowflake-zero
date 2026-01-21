# Zero to Snowflake Automation

This project automates the "Zero to Snowflake" quickstart guide, including setting up Snowflake objects, running SQL vignettes, and deploying a Streamlit application.

## Prerequisites

- **Python 3.8+**: Ensure you have Python installed.
- **Snowflake Account**: You need a Snowflake account with `ACCOUNTADMIN` access.
- **Git**: For version control.

## Installation

1.  **Clone the Repository** (if not already done):
    ```bash
    git clone https://github.com/ranjithvijik/snowflake-zero.git
    cd snowflake
    ```

2.  **Set up a Virtual Environment** (recommended):
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\\Scripts\\activate
    ```

3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

You can provide Snowflake credentials interactively when running the scripts, or set them as environment variables (recommended for automation).

**Environment Variables:**

*   `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`).
*   `SNOWFLAKE_USER`: Your Snowflake username.
*   `SNOWFLAKE_PASSWORD`: Your Snowflake password.
*   `SNOWFLAKE_ROLE`: Role to use (default: `ACCOUNTADMIN`).
*   `SNOWFLAKE_WAREHOUSE`: Warehouse to use (default: `COMPUTE_WH`).
*   `SNOWFLAKE_DATABASE`: Database name (default: `TB_101`).
*   `SNOWFLAKE_SCHEMA`: Schema name (default: `ANALYTICS`).

## Running the Automation

The automation script executes the SQL vignettes in order (00_setup, 01_vignette, etc.) to provision your Snowflake environment.

1.  **Run the Automation Script**:
    ```bash
    python snowflake_automation/run_all.py
    ```
    *   If environment variables are not set, you will be prompted to enter your credentials.
    *   The script will connect to Snowflake and execute the SQL files in `snowflake_automation/scripts/`.

## Running the Streamlit App

Once the Snowflake environment is set up (specifically after `00_setup.sql` is run), you can launch the Streamlit application.

1.  **Run the App**:
    ```bash
    streamlit run app.py
    ```

2.  **Access the App**:
    *   The app will open in your default browser (usually at `http://localhost:8501`).
    *   It visualizes the "Zero to Snowflake" data (e.g., Daily Sales).

## Project Structure

*   `app.py`: The main Streamlit application.
*   `snowflake_automation/`:
    *   `run_all.py`: Main script to execute the SQL automation.
    *   `utils.py`: Utility functions for database connection and SQL execution.
    *   `scripts/`: Directory containing the SQL vignettes (`00_setup.sql`, `01_vignette_1.sql`, etc.).
*   `requirements.txt`: Python dependencies.
