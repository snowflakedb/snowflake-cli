import streamlit as st
from common.hello import say_hello

st.title(f"Example streamlit app. {say_hello()}")
