import streamlit as st

st.title("Timescale Performance vs PostgreSQL")
st.write(
    "For help head over to [docs.streamlit.io](https://docs.streamlit.io/)."
)
st.write(
    "Querying 10M rides table from NYC can dataset"
)

st.write(
    "SQL: SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;"
)


# streamlit_app.py

import streamlit as st

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;', ttl="0")

# Print results.
st.write("Hypertable Query")
st.dataframe(df, use_container_width=True)

# Perform query.
df_pg = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides_pg_table GROUP BY rate_code ORDER BY rate_code;', ttl="0")

# Print results.
st.write("PG Table Query")
st.dataframe(df_pg, use_container_width=True)