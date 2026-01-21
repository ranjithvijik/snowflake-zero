import urllib.request
import os

SCRIPTS_DIR = "snowflake_automation/scripts"
os.makedirs(SCRIPTS_DIR, exist_ok=True)

files = {
    "00_setup.sql": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/scripts/setup.sql",
    "01_vignette_1.sql": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/scripts/vignette-1.sql",
    "02_vignette_2.sql": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/scripts/vignette-2.sql",
    "03_vignette_3.sql": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/scripts/vignette-3-aisql.sql",
    "04_vignette_4.sql": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/scripts/vignette-4.sql",
    "05_vignette_5.sql": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/scripts/vignette-5.sql",
    "06_streamlit_app.py": "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/main/streamlit/streamlit_app.py"
}

for filename, url in files.items():
    print(f"Downloading {filename}...")
    try:
        with urllib.request.urlopen(url) as response:
            content = response.read().decode('utf-8')
            with open(os.path.join(SCRIPTS_DIR, filename), 'w') as f:
                f.write(content)
        print(f"Saved {filename}")
    except Exception as e:
        print(f"Failed to download {filename}: {e}")
