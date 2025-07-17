from snowflake.snowpark import Session
import streamlit as st

def get_snowflake_session():
    connection_parameters = {
        "account": st.secrets["account"],
        "user": st.secrets["user"],
        "role": st.secrets["role"],
        "warehouse": st.secrets["warehouse"],
        "database": st.secrets["database"],
        "schema": st.secrets["schema"],
        "authenticator": "externalbrowser",
        #"authenticator": "oauth",
        # "token": st.secrets["token"],  # uncomment if you're passing an OAuth access token
        #"password": st.secrets["password"],
    }
    return Session.builder.configs(connection_parameters).create()
