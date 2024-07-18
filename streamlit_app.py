import streamlit as st
import time

st.title("TimescaleDB vs PostgreSQL Bake-Off")
st.write("For help head over to [docs.streamlit.io](https://docs.streamlit.io/).")
st.write("Querying 10M rides table from NYC cab dataset")
st.write("SQL Executed: SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform hypertable query.
hypertable_start_time = time.time()
df = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;', ttl="0")
hypertable_end_time = time.time()

# Perform pg query.
postgresql_start_time = time.time()
df_pg = conn.query('SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides_pg_table GROUP BY rate_code ORDER BY rate_code;', ttl="0")
postgresql_end_time = time.time()

# Print results.
data_container = st.container()

with data_container:
    postgresql, timescale  = st.columns(2)
    with timescale:
        st.subheader("Hypertable Query - Elapsed {0:4.1f}s".format ((hypertable_end_time - hypertable_start_time)))  
        st.dataframe(df.set_index(df.columns[0]))
        st.write(f"**Convert standard table into hypertable partitioned on the time column:**")
        st.write("SELECT create_hypertable('rides', by_range('pickup_datetime'), create_default_indexes=>FALSE);")
        st.write(f"**Additional partitioning dimension to speed up queries:**")
        st.write("SELECT create_hypertable('rides', by_range('pickup_datetime'), create_default_indexes=>FALSE);")

    with postgresql:
        st.subheader("PostgreSQL Query - Elapsed {0:4.1f}s".format ((postgresql_end_time - postgresql_start_time))) 
        st.dataframe(df_pg.set_index(df_pg.columns[0]))
        st.write(f"**Create a pg table:**")
        st.write("CREATE TABLE  rides (vendor_id TEXT, pickup_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, dropoff_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, passenger_count NUMERIC, trip_distance NUMERIC, pickup_longitude NUMERIC, pickup_latitude NUMERIC, rate_code INTEGER, dropoff_longitude NUMERIC, dropoff_latitude  NUMERIC, payment_type INTEGER, fare_amount NUMERIC, extra NUMERIC, mta_tax NUMERIC, tip_amount NUMERIC, tolls_amount NUMERIC, improvement_surcharge NUMERIC, total_amount NUMERIC);")