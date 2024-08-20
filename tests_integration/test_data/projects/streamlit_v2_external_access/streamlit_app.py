import streamlit as st
import _snowflake


st.title(f"Example streamlit app.")
secret = _snowflake.get_generic_secret_string("generic_secret")
