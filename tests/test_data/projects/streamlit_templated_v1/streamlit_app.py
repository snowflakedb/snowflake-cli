from common.hello import say_hello

import streamlit as st

st.title(f"Example streamlit app. {say_hello()}")
