import streamlit as st
from utils.utils import hello_world

st.title("Simple Page")
st.write("This page demonstrates basic Streamlit functionality in SPCS runtime v2.")

# Simple content
st.header("Page Content")
st.write(hello_world())

# Simple interactive elements
st.header("Interactive Elements")
name = st.text_input("Enter your name:", "World")
st.write(f"Hello, {name}!")

# Simple columns layout
col1, col2 = st.columns(2)

with col1:
    st.subheader("Column 1")
    st.write("This is the first column")

with col2:
    st.subheader("Column 2")
    st.write("This is the second column")

# Simple metrics
st.header("Simple Metrics")
st.metric("Test Metric", 42)
st.metric("Another Metric", 100, delta=10)
