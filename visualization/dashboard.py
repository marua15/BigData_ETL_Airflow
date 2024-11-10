import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px 

# Database connection details
conn = None
try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    st.success("Connection successful!")
except psycopg2.OperationalError as e:
    st.error(f"OperationalError: {e}")

# Query function
def query_database(sql_query):
    with conn.cursor() as cur:
        cur.execute(sql_query)
        data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return pd.DataFrame(data, columns=columns)

# Streamlit Dashboard Interface
st.title("MasterCard vs Visa: Data Insights Dashboard")

# Sidebar for query selection
st.sidebar.header("Query Options")

# Allow user to select table and limit results
table_name = st.sidebar.text_input("Enter table name", "MVR")
limit = st.sidebar.number_input("Limit results", min_value=1, max_value=1000, value=100, step=10)

# Dynamic query based on user input
if conn:
    query = f"SELECT * FROM \"{table_name}\" LIMIT {limit};"
    df = query_database(query)
    
    # Display the dataframe
    st.write("Data Preview", df)
    
    # Column selection
    columns_to_display = st.multiselect("Select columns to display", options=df.columns, default=df.columns)
    filtered_df = df[columns_to_display]
    
    # Create layout with columns for different sections
    col1, col2 = st.columns(2)
    
    with col1:
        # Price Insights Section (e.g., Closing Prices)
        st.header("Price Insights")
        st.subheader("MasterCard vs Visa: Closing Prices")
        fig = px.line(filtered_df, x='Date', y=['Close_M', 'Close_V'], 
                      labels={'Date': 'Date', 'value': 'Price'}, 
                      title='Closing Prices for MasterCard and Visa')
        st.plotly_chart(fig)

        # Price Change Over Time
        st.subheader("Price Change")
        fig_pct_change = px.line(filtered_df, x='Date', y=['Pct_Change_M', 'Pct_Change_V'],
                                 labels={'Date': 'Date', 'value': 'Percentage Change'},
                                 title='Percentage Change of Prices Over Time')
        st.plotly_chart(fig_pct_change)

        # Risk & Volatility
        st.header("Risk and Volatility Insights")
        st.subheader("Price Volatility Over Time")
        fig_volatility = px.line(filtered_df, x='Date', y=['Volatility_M', 'Volatility_V'],
                                 labels={'Date': 'Date', 'value': 'Volatility'},
                                 title='Price Volatility for MasterCard and Visa')
        st.plotly_chart(fig_volatility)

    with col2:
        # Volume Insights Section
        st.header("Volume Insights")
        st.subheader("Transaction Volumes")
        fig_volumes = px.bar(filtered_df, x='Date', y=['Volume_M', 'Volume_V'],
                             title="Transaction Volumes for MasterCard and Visa")
        st.plotly_chart(fig_volumes)

        # Moving Averages: Price & Volume
        st.subheader("Moving Averages for Prices and Volumes")
        fig_moving_avg = px.line(filtered_df, x='Date', y=['MA7_Close_M', 'MA7_Close_V', 'MA30_Close_M', 'MA30_Close_V'],
                                  title='7-Day and 30-Day Moving Averages for Closing Prices')
        st.plotly_chart(fig_moving_avg)

        # Volume Moving Averages
        st.subheader("Volume Moving Averages")
        fig_volume_ma = px.line(filtered_df, x='Date', y=['Volume_MA7_M', 'Volume_MA7_V'],
                                title="7-Day Moving Average of Volumes")
        st.plotly_chart(fig_volume_ma)

    # Additional Insights Section
    st.header("Advanced Insights")
    st.subheader("Volume to Price Ratio")
    fig_vol_price_ratio = px.line(filtered_df, x='Date', y='Volume_Ratio_MV',
                                  title="Volume to Price Ratio for MasterCard and Visa")
    st.plotly_chart(fig_vol_price_ratio)

    # Decision-Making Insights
    st.header("Market Sentiment & Decision Making")
    st.subheader("Price Change vs Volume for Market Sentiment")
    fig_sentiment = px.scatter(filtered_df, x='Pct_Change_M', y='Volume_Ratio_MV', color='Date',
                               labels={'Pct_Change_M': 'Price Change', 'Volume_Ratio_MV': 'Volume to Price Ratio'},
                               title='Market Sentiment: Price Change vs Volume')
    st.plotly_chart(fig_sentiment)

else:
    st.warning("Connection to the database could not be established.")