import streamlit as st
from utils.utils import generate_sample_data

st.title("My SPCS Streamlit App")
st.write("This app is running on SPCS runtime v2!")

# Generate sample data using numpy and pandas
st.header("Sample Data Analysis")
df = generate_sample_data()
st.dataframe(df)

# Show some basic statistics
st.header("Data Statistics")
st.write(f"Dataset shape: {df.shape}")
st.write(f"Mean values:\n{df.mean()}")

# Create a simple chart
st.header("Data Visualization")
st.line_chart(df)
