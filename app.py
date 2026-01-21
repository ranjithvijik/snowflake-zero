import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, desc, to_date

# --- App Setup and Authentication ---
st.set_page_config(layout="wide")
st.title("Tasty Bytes - Dashboard & AI Demo")

@st.cache_resource
def create_session():
    try:
        return get_active_session()
    except Exception:
        if "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
            return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
        else:
            st.error("Missing Snowflake credentials in st.secrets. Please configure them to run locally.")
            return None

session = create_session()
if not session:
    st.stop()

# --- Tab Layout ---
tab1, tab2, tab3 = st.tabs(["📊 Sales Analysis", "💬 Customer Reviews", "🧠 Cortex AI"])

# --- TAB 1: Sales Analysis (Refactored for Push-Down) ---
with tab1:
    st.header("Sales Performance")
    
    # We no longer load the raw dataframe here. We run specific aggregate queries.
    
    # Define base Snowpark DataFrame (Lazy evaluation - no data moves yet)
    base_orders_df = session.table("TB_101.ANALYTICS.ORDERS_V")

    # 1. Total Sales by City (Aggregated in Snowflake)
    st.subheader("Total Sales by City")
    try:
        city_sales_snow = (
            base_orders_df
            .group_by("PRIMARY_CITY")
            .agg(sum_("ORDER_TOTAL").alias("ORDER_TOTAL"))
            .sort(desc("ORDER_TOTAL"))
        )
        # .to_pandas() triggers the query execution and downloads only the result
        city_sales_pd = city_sales_snow.to_pandas()

        city_chart = alt.Chart(city_sales_pd).mark_bar().encode(
            x=alt.X('ORDER_TOTAL:Q', title='Total Sales ($)'),
            y=alt.Y('PRIMARY_CITY:N', sort='-x', title='City'),
            tooltip=['PRIMARY_CITY', 'ORDER_TOTAL']
        ).properties(height=400)
        st.altair_chart(city_chart, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            # 2. Daily Sales Trend (Aggregated in Snowflake)
            st.subheader("Daily Sales Trend")
            daily_sales_snow = (
                base_orders_df
                .group_by("DATE")
                .agg(sum_("ORDER_TOTAL").alias("ORDER_TOTAL"))
                .sort("DATE")
            )
            daily_sales_pd = daily_sales_snow.to_pandas()
            
            daily_chart = alt.Chart(daily_sales_pd).mark_line(point=True).encode(
                x=alt.X('DATE:T', title='Date'),
                y=alt.Y('ORDER_TOTAL:Q', title='Sales ($)'),
                tooltip=['DATE', 'ORDER_TOTAL']
            ).properties(height=300)
            st.altair_chart(daily_chart, use_container_width=True)

        with col2:
            # 3. Top Menu Items (Aggregated & Limited in Snowflake)
            st.subheader("Top 10 Menu Items")
            top_items_snow = (
                base_orders_df
                .group_by("MENU_ITEM_NAME")
                .agg(sum_("ORDER_TOTAL").alias("ORDER_TOTAL"))
                .sort(desc("ORDER_TOTAL"))
                .limit(10)
            )
            top_items_pd = top_items_snow.to_pandas()
            
            item_chart = alt.Chart(top_items_pd).mark_bar().encode(
                x=alt.X('ORDER_TOTAL:Q', title='Total Sales ($)'),
                y=alt.Y('MENU_ITEM_NAME:N', sort='-x', title='Menu Item'),
                tooltip=['MENU_ITEM_NAME', 'ORDER_TOTAL']
            ).properties(height=300)
            st.altair_chart(item_chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error computing sales analytics: {e}")

# --- TAB 2: Customer Reviews ---
with tab2:
    st.header("Customer Feedback")
    
    # We still use a Limit here to keep it safe, but this is less heavy than Orders
    @st.cache_data
    def load_reviews_data():
        try:
            return session.table("TB_101.ANALYTICS.TRUCK_REVIEWS_V").select(
                col("DATE"), col("TRUCK_BRAND_NAME"), col("REVIEW"), col("LANGUAGE")
            ).limit(1000).to_pandas()
        except Exception as e:
            st.error(f"Error reading review data: {e}")
            return pd.DataFrame()

    reviews_df = load_reviews_data()
    
    if not reviews_df.empty:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Reviews by Brand")
            brand_counts = reviews_df['TRUCK_BRAND_NAME'].value_counts().reset_index()
            brand_counts.columns = ['Brand', 'Count']
            
            brand_chart = alt.Chart(brand_counts).mark_bar().encode(
                x=alt.X('Count:Q', title='Number of Reviews'),
                y=alt.Y('Brand:N', sort='-x', title='Truck Brand'),
                color='Brand',
                tooltip=['Brand', 'Count']
            ).properties(height=400)
            st.altair_chart(brand_chart, use_container_width=True)
            
        with col2:
            st.subheader("Recent Reviews")
            st.dataframe(reviews_df[['DATE', 'TRUCK_BRAND_NAME', 'REVIEW']].head(50), use_container_width=True)
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