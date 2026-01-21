# Welcome to Streamlit in Snowflake!

# Import necessary libraries
# streamlit is used for creating the web app interface.
import streamlit as st
# pandas is used for data manipulation and analysis.
import pandas as pd
# altair is used for creating interactive data visualizations.
import altair as alt
# snowflake.snowpark.context is used to connect to Snowflake and get the active session.
# snowflake.snowpark.context is used to connect to Snowflake and get the active session.
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

# --- App Setup and Data Loading ---

st.title("Menu Item Sales in Japan for February 2022")
st.write('---')

# @st.cache_resource is better for storing the database session itself
@st.cache_resource
def create_session():
    """
    1. Tries to get the active session (works inside Snowflake).
    2. If that fails, creates a session using credentials from st.secrets (works in Streamlit Cloud).
    """
    try:
        return get_active_session()
    except Exception:
        # If running locally or on Streamlit Cloud, use st.secrets
        # Ensure your secrets.toml has a [connections.snowflake] section
        if "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
            return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
        else:
            st.error("Missing Snowflake credentials in st.secrets")
            return None

session = create_session()

# --- Data Loading Function ---

@st.cache_data
def load_data():
    if session:
        # Fetch data using Snowpark
        try:
            # Using the full path ensures we hit the right table regardless of default schema
            japan_sales_df = session.table("TB_101.ANALYTICS.japan_menu_item_sales_feb_2022").to_pandas()
            return japan_sales_df
        except Exception as e:
            st.error(f"Error reading table: {e}")
            return pd.DataFrame() # Return empty DF on failure
    return pd.DataFrame()

japan_sales = load_data()

# --- Stop Execution if Data is Empty ---
if japan_sales.empty:
    st.warning("No data loaded. Please check your Snowflake connection settings.")
    st.stop()

# --- User Interaction with Widgets ---

# Get a unique list of menu item names from the DataFrame to populate the dropdown.
menu_item_names = japan_sales['MENU_ITEM_NAME'].unique().tolist()

# Create a dropdown menu (selectbox) in the Streamlit sidebar or main page.
# The user's selection will be stored in the 'selected_menu_item' variable.
selected_menu_item = st.selectbox("Select a menu item", options=menu_item_names)


# --- Data Setup ---

# Filter the main DataFrame to include only the rows that match the user's selected menu item.
menu_item_sales = japan_sales[japan_sales['MENU_ITEM_NAME'] == selected_menu_item]

# Group the filtered data by 'DATE' and calculate the sum of 'ORDER_TOTAL' for each day. 
daily_totals = menu_item_sales.groupby('DATE')['ORDER_TOTAL'].sum().reset_index()


# --- Chart Setup ---

# Calculate the range of sales values to set a dynamic y-axis scale.
min_value = daily_totals['ORDER_TOTAL'].min()
max_value = daily_totals['ORDER_TOTAL'].max()

# Calculate a margin to add above and below the min/max values on the chart.
chart_margin = (max_value - min_value) / 2
y_margin_min = min_value - chart_margin
y_margin_max = max_value + chart_margin

# Create a line chart.
chart = alt.Chart(daily_totals).mark_line(
    point=True,     
    tooltip=True
).encode(
    x=alt.X('DATE:T',
            axis=alt.Axis(title='Date', format='%b %d'),
            title='Date'),
    y=alt.Y('ORDER_TOTAL:Q',
            axis=alt.Axis(title='Total Sales ($)'), 
            title='Total Daily Sales',
# Set a custom domain (range) for the y-axis to add padding dynamically. 
            scale=alt.Scale(domain=[y_margin_min, y_margin_max]))
).properties(
    title=f'Total Daily Sales for Menu Item: {selected_menu_item}',
    height=500
)


# --- Displaying the Chart ---

# Render the Altair chart in the Streamlit app.
# 'use_container_width=True' makes the chart expand to the full width of the container.
st.altair_chart(chart, width="stretch")