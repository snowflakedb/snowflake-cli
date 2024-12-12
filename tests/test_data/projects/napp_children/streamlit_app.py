from http.client import HTTPSConnection

import _snowflake
import streamlit as st


def get_secret_value():
    return _snowflake.get_generic_secret_string("generic_secret")


def send_request():
    host = "docs.snowflake.com"
    conn = HTTPSConnection(host)
    conn.request("GET", "/")
    response = conn.getresponse()
    st.success(f"Response status: {response.status}")


st.title(f"Example streamlit app.")
st.button("Send request", on_click=send_request)
