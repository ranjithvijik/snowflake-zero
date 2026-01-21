import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, desc, count

# --- App Setup and Authentication ---
st.set_page_config(layout="wide")
st.title("Tasty Bytes - Dashboard & AI Demo")

@st.cache_resource
def create_session():
    """
    Creates a Snowpark session.
    Prioritizes st.secrets (Streamlit Cloud) and allows fallback to simple authentication.
    """
    try:
        # Check for secrets structure (Streamlit Cloud standard)
        if "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
            return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
        # Fallback for alternative secrets structure
        elif "snowflake" in st.secrets:
             return Session.builder.configs(st.secrets["snowflake"]).create()
        else:
             # Try getting active session (SiS) as last resort
             return get_active_session()
    except Exception as e:
        st.error(f"Failed to create Snowflake session. Check your secrets configuration. Error: {e}")
        return None

session = create_session()
if not session:
    st.stop()

# --- Data Functions (Push-Down Aggregations) ---
# All calculations are performed in Snowflake. Only summary data is downloaded.

@st.cache_data
def get_city_sales():
    """Aggregates total sales by city in Snowflake."""
    try:
        df = (
            session.table("TB_101.ANALYTICS.ORDERS_V")
            .group_by("PRIMARY_CITY")
            .agg(sum_("ORDER_TOTAL").alias("ORDER_TOTAL"))
            .sort(desc("ORDER_TOTAL"))
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching city sales: {e}")
        return pd.DataFrame()

@st.cache_data
def get_daily_sales():
    """Aggregates daily sales trend in Snowflake."""
    try:
        df = (
            session.table("TB_101.ANALYTICS.ORDERS_V")
            .group_by("DATE")
            .agg(sum_("ORDER_TOTAL").alias("ORDER_TOTAL"))
            .sort("DATE")
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching daily sales: {e}")
        return pd.DataFrame()

@st.cache_data
def get_top_items():
    """Finds top 10 menu items by sales in Snowflake."""
    try:
        df = (
            session.table("TB_101.ANALYTICS.ORDERS_V")
            .group_by("MENU_ITEM_NAME")
            .agg(sum_("ORDER_TOTAL").alias("ORDER_TOTAL"))
            .sort(desc("ORDER_TOTAL"))
            .limit(10)
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching top items: {e}")
        return pd.DataFrame()

@st.cache_data
def get_reviews_by_brand():
    """Counts reviews per brand in Snowflake."""
    try:
        df = (
            session.table("TB_101.ANALYTICS.TRUCK_REVIEWS_V")
            .group_by("TRUCK_BRAND_NAME")
            .agg(count("*").alias("COUNT"))
            .sort(desc("COUNT"))
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching review counts: {e}")
        return pd.DataFrame()

@st.cache_data
def get_recent_reviews():
    """Fetches the 50 most recent reviews from Snowflake."""
    try:
        df = (
            session.table("TB_101.ANALYTICS.TRUCK_REVIEWS_V")
            .select("DATE", "TRUCK_BRAND_NAME", "REVIEW")
            .sort(desc("DATE"))
            .limit(50)
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching recent reviews: {e}")
        return pd.DataFrame()

# --- Layout & Features ---

tab1, tab2, tab3 = st.tabs(["📊 Sales Analysis", "💬 Customer Reviews", "🧠 Cortex AI"])

# --- TAB 1: Sales Analysis ---
with tab1:
    st.header("Sales Performance")
    
    # Load Summary Data
    city_sales_df = get_city_sales()
    daily_sales_df = get_daily_sales()
    top_items_df = get_top_items()

    if not city_sales_df.empty:
        # 1. Total Sales by City
        st.subheader("Total Sales by City")
        city_chart = alt.Chart(city_sales_df).mark_bar().encode(
            x=alt.X('ORDER_TOTAL:Q', title='Total Sales ($)'),
            y=alt.Y('PRIMARY_CITY:N', sort='-x', title='City'),
            tooltip=['PRIMARY_CITY', 'ORDER_TOTAL']
        ).properties(height=400)
        st.altair_chart(city_chart, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
             # 2. Daily Sales Trend
            st.subheader("Daily Sales Trend")
            if not daily_sales_df.empty:
                daily_chart = alt.Chart(daily_sales_df).mark_line(point=True).encode(
                    x=alt.X('DATE:T', title='Date'),
                    y=alt.Y('ORDER_TOTAL:Q', title='Sales ($)'),
                    tooltip=['DATE', 'ORDER_TOTAL']
                ).properties(height=300)
                st.altair_chart(daily_chart, use_container_width=True)

        with col2:
            # 3. Top Menu Items
            st.subheader("Top 10 Menu Items")
            if not top_items_df.empty:
                item_chart = alt.Chart(top_items_df).mark_bar().encode(
                    x=alt.X('ORDER_TOTAL:Q', title='Total Sales ($)'),
                    y=alt.Y('MENU_ITEM_NAME:N', sort='-x', title='Menu Item'),
                    tooltip=['MENU_ITEM_NAME', 'ORDER_TOTAL']
                ).properties(height=300)
                st.altair_chart(item_chart, use_container_width=True)
    else:
        st.info("No sales data available.")

# --- TAB 2: Customer Reviews ---
with tab2:
    st.header("Customer Feedback")
    
    brand_counts_df = get_reviews_by_brand()
    recent_reviews_df = get_recent_reviews()
    
    if not brand_counts_df.empty:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Reviews by Brand")
            # Note: Snowpark returns uppercase column names usually, or preserves case if quoted.
            # adjusting column names for chart if needed, but Snowpark mostly returns as is from alias.
            # Using specific column names from the DF to be safe.
            
            brand_chart = alt.Chart(brand_counts_df).mark_bar().encode(
                x=alt.X('COUNT:Q', title='Number of Reviews'),
                y=alt.Y('TRUCK_BRAND_NAME:N', sort='-x', title='Truck Brand'),
                color='TRUCK_BRAND_NAME',
                tooltip=['TRUCK_BRAND_NAME', 'COUNT']
            ).properties(height=400)
            st.altair_chart(brand_chart, use_container_width=True)
            
        with col2:
            st.subheader("Recent Reviews")
            st.dataframe(recent_reviews_df, use_container_width=True)
    else:
        st.info("No review data available.")

# --- TAB 3: Cortex AI ---
with tab3:
    st.header("Snowflake Cortex AI Demo")
    st.markdown("""
    Context: Use **Snowflake Cortex** to instantly analyze sentiment. 
    Enter a hypothetical review below to see how the AI scores it (-1 to 1).
    """)
    
    user_input = st.text_area("Enter a food truck review:", placeholder="The tacos were amazing but the line was too long!")
    
    if user_input:
        if st.button("Analyze Sentiment"):
            try:
                # Escaping single quotes for basic SQL safety in demo
                safe_input = user_input.replace("'", "''") 
                query = f"SELECT SNOWFLAKE.CORTEX.SENTIMENT('{safe_input}') AS SENTIMENT_SCORE"
                
                result = session.sql(query).collect()
                score = result[0]['SENTIMENT_SCORE']
                
                st.metric(label="Sentiment Score", value=f"{score:.2f}")
                
                if score > 0.2:
                    st.success("Positive Feedback! 😊")
                elif score < -0.2:
                    st.error("Negative Feedback 😞")
                else:
                    st.warning("Neutral Feedback 😐")
            except Exception as e:
                st.error(f"Error running Cortex AI: {e}")