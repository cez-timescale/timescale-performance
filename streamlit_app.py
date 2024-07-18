import streamlit as st
import time

st.title("TimescaleDB vs PostgreSQL Bake-Off")
st.write("For help head over to [docs.streamlit.io](https://docs.streamlit.io/).")
st.write("Querying 10M rides table from NYC cab dataset")
st.write("SQL Executed: SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform hypertable query.
start_time = time.time()
df = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;', ttl="0")
end_time = time.time()

# Print results.
st.write("Hypertable Query Complete - Elapsed {0:4.1f}s".format ((end_time - start_time)))  
st.dataframe(df.set_index(df.columns[0]))

# Perform pg query.
start_time = time.time()
df_pg = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides_pg_table GROUP BY rate_code ORDER BY rate_code;', ttl="0")
end_time = time.time()

# Print results.
st.write("PostgreSQL Query Complete - Elapsed {0:4.1f}s".format ((end_time - start_time))) 
st.dataframe(df_pg.set_index(df_pg.columns[0]))

data_container = st.container()

with data_container:
    timescale, postgresql = st.columns(2)
    with timescale:
        st.write("Hypertable Query Complete - Elapsed {0:4.1f}s".format ((end_time - start_time)))  
        st.dataframe(df.set_index(df.columns[0]))
    with postgresql:
        st.write("PostgreSQL Query Complete - Elapsed {0:4.1f}s".format ((end_time - start_time))) 
        st.dataframe(df_pg.set_index(df_pg.columns[0]))