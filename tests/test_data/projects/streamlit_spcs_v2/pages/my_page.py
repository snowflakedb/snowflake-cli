import numpy as np
import pandas as pd
import streamlit as st
from utils.utils import calculate_moving_average, generate_sample_data

st.title("Data Analysis Page")
st.write(
    "This page demonstrates advanced data analysis with pandas and numpy in SPCS runtime v2."
)

# Load the same dataset
df = generate_sample_data()

# Show correlation analysis
st.header("Correlation Analysis")
correlation_matrix = df.corr()
st.dataframe(correlation_matrix)

# Moving averages
st.header("Moving Averages")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Temperature (7-day MA)")
    temp_ma = calculate_moving_average(df["temperature"])
    chart_data = pd.DataFrame(
        {"Original": df["temperature"], "Moving Average": temp_ma}
    )
    st.line_chart(chart_data)

with col2:
    st.subheader("Humidity Distribution")
    st.histogram(df["humidity"], bins=20)

# Summary statistics
st.header("Advanced Statistics")
st.write(f"**Temperature Stats:**")
st.write(f"- Standard deviation: {df['temperature'].std():.2f}")
st.write(f"- Skewness: {df['temperature'].skew():.2f}")
st.write(f"- 95th percentile: {np.percentile(df['temperature'], 95):.2f}")
