import streamlit as st
import time

st.title("TimescaleDB vs PostgreSQL Perf")
st.write("For help head over to [docs.streamlit.io](https://docs.streamlit.io/).")
st.write("Querying 10M rides table from NYC can dataset")
st.write("SQL: SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform hypertable query.
start_time = time.time()
df = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;', ttl="0")
end_time = time.time()

# Print results.
st.write("Hypertable Query")
st.write("Hyoertable Query Complete - Elapsed {0:4.1f}s".format ((end_time - start_time)))  
st.dataframe(df, use_container_width=True)


# Perform pg query.
df_pg = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides_pg_table GROUP BY rate_code ORDER BY rate_code;', ttl="0")

# Print results.
st.write("PG Table Query")
st.dataframe(df_pg, use_container_width=True)