import streamlit as st
from utils.utils import hello_world

st.title("My SPCS Streamlit App")
st.write("This app is running on SPCS runtime v2!")

# Simple test content
st.header("Basic Functionality Test")
st.write(hello_world())

# Simple data display
st.header("Simple Data")
simple_data = {
    "Name": ["Alice", "Bob", "Charlie"],
    "Age": [25, 30, 35],
    "City": ["New York", "San Francisco", "Seattle"],
}
st.table(simple_data)

# Simple chart with built-in data
st.header("Simple Chart")
chart_data = [1, 3, 2, 4, 5, 3, 2]
st.line_chart(chart_data)
