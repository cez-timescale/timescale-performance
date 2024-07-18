import streamlit as st

st.title("🎈 My new app")
st.write(
    "Let's start building! For help and inspiration, head over to [docs.streamlit.io](https://docs.streamlit.io/)."
)

# streamlit_app.py

import streamlit as st

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;', ttl="0")

# Print results.
st.dataframe(df, use_container_width=True)

# Perform query.
df_pg = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides_pg_table GROUP BY rate_code ORDER BY rate_code;', ttl="0")

# Print results.
st.dataframe(df_pg, use_container_width=True)