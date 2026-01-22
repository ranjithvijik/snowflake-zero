import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, desc, count

# --- App Setup and Authentication ---
st.set_page_config(layout="wide")
st.title("Tasty Bytes")

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

@st.cache_data
def get_loyalty_demographics():
    """Aggregates sales by marital status and gender."""
    try:
        # Using ORDERS_V as it contains joined loyalty info + sales
        df = (
            session.table("TB_101.ANALYTICS.ORDERS_V")
            .filter(col("MARITAL_STATUS").is_not_null())
            .group_by("MARITAL_STATUS")
            .agg(sum_("ORDER_TOTAL").alias("TOTAL_SALES"))
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching demographics: {e}")
        return pd.DataFrame()

@st.cache_data
def get_top_customers():
    """Fetches top 10 customers by lifetime spend."""
    try:
        df = (
            session.table("TB_101.ANALYTICS.CUSTOMER_LOYALTY_METRICS_V")
            .select("FIRST_NAME", "LAST_NAME", "CITY", "TOTAL_SALES")
            .sort(desc("TOTAL_SALES"))
            .limit(10)
        ).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching top customers: {e}")
        return pd.DataFrame()

# --- Layout & Features ---

tab1, tab2, tab3, tab4 = st.tabs(["📊 Sales", "🏆 Loyalty", "💬 Reviews", "🧠 Cortex AI"])

# --- TAB 1: Sales Analysis ---
with tab1:
    st.header("Sales Performance")
    
    city_sales_df = get_city_sales()
    daily_sales_df = get_daily_sales()
    top_items_df = get_top_items()

    if not city_sales_df.empty:
        st.subheader("Total Sales by City")
        city_chart = alt.Chart(city_sales_df).mark_bar().encode(
            x=alt.X('ORDER_TOTAL:Q', title='Total Sales ($)'),
            y=alt.Y('PRIMARY_CITY:N', sort='-x', title='City'),
            tooltip=['PRIMARY_CITY', 'ORDER_TOTAL']
        ).properties(height=400)
        st.altair_chart(city_chart, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Daily Sales Trend")
            if not daily_sales_df.empty:
                daily_chart = alt.Chart(daily_sales_df).mark_line(point=True).encode(
                    x=alt.X('DATE:T', title='Date'),
                    y=alt.Y('ORDER_TOTAL:Q', title='Sales ($)'),
                    tooltip=['DATE', 'ORDER_TOTAL']
                ).properties(height=300)
                st.altair_chart(daily_chart, use_container_width=True)

        with col2:
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

# --- TAB 2: Loyalty Analytics ---
with tab2:
    st.header("Loyalty Program Insights")
    
    demo_df = get_loyalty_demographics()
    top_cust_df = get_top_customers()
    
    if not demo_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Sales by Marital Status")
            donut = alt.Chart(demo_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta("TOTAL_SALES", stack=True),
                color=alt.Color("MARITAL_STATUS"),
                tooltip=["MARITAL_STATUS", "TOTAL_SALES"]
            ).properties(height=350)
            st.altair_chart(donut, use_container_width=True)
            
        with col2:
            st.subheader("Top 10 VIP Customers")
            st.dataframe(top_cust_df, use_container_width=True, hide_index=True)
            
    else:
        st.info("No loyalty data available.")

# --- TAB 3: Customer Reviews ---
with tab3:
    st.header("Customer Feedback")
    
    brand_counts_df = get_reviews_by_brand()
    recent_reviews_df = get_recent_reviews()
    
    if not brand_counts_df.empty:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Reviews by Brand")
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

# --- TAB 4: Cortex AI Playground ---
with tab4:
    st.header("🧠 Cortex AI")
    
    ai_choice = st.selectbox("Select AI Capability:", 
                             ["Sentiment Analysis", "Translation", "Summarization", "Idea Extraction"])
    
    user_input = st.text_area("Input Text (e.g., a review):", height=100,
                              placeholder="The burgers were delicious but the service was a bit slow.")
    
    safe_input = user_input.replace("'", "''") if user_input else ""
    
    # Display target language option if Translation is selected
    target_lang = "es"
    if ai_choice == "Translation":
        target_lang = st.selectbox("Target Language", ["es", "fr", "de", "it", "ja", "ko"])

    if st.button("Run AI"):
        if not user_input:
            st.warning("Please enter some text first.")
        else:
            try:
                if ai_choice == "Sentiment Analysis":
                    query = f"SELECT SNOWFLAKE.CORTEX.SENTIMENT('{safe_input}') AS VAL"
                    val = session.sql(query).collect()[0]['VAL']
                    st.metric("Sentiment Score (-1 to 1)", f"{val:.2f}")
                    if val > 0.2: st.success("Positive")
                    elif val < -0.2: st.error("Negative")
                    else: st.warning("Neutral")

                elif ai_choice == "Translation":
                    # Uses the target_lang selected above
                    query = f"SELECT SNOWFLAKE.CORTEX.TRANSLATE('{safe_input}', 'en', '{target_lang}') AS VAL"
                    val = session.sql(query).collect()[0]['VAL']
                    st.subheader("Translation:")
                    st.write(val)
                    
                elif ai_choice == "Summarization":
                    query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{safe_input}') AS VAL"
                    val = session.sql(query).collect()[0]['VAL']
                    st.subheader("Summary:")
                    st.write(val)

                elif ai_choice == "Idea Extraction":
                    query = f"SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER('{safe_input}', 'What was good and what was bad?') AS VAL"
                    val = session.sql(query).collect()[0]['VAL']
                    st.subheader("Key Points:")
                    st.write(val)
                    
            except Exception as e:
                 st.error(f"Error running Cortex AI: {e}")