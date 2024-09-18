import os
import sys

import streamlit as st

# Add the path to the 'utils' directory containing helper.py
sys.path.append(os.path.join(os.getcwd(), "utils"))

# Import functions from staged files
from helper import greet_user

# The rest of your Streamlit app code
st.title("Example streamlit app")

# Use the imported function
user_greeting = greet_user("Vida")
