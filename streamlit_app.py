import streamlit as st
import time

st.title("TimescaleDB vs PostgreSQL")
st.write("Try Timescale for Free [Link](https://www.timescale.com/?utm_source=google&utm_medium=cpc&utm_campaign=brand&utm_term=timescale&utm_campaign=g_search_brand&utm_source=adwords&utm_medium=ppc&hsa_acc=9771591554&hsa_cam=20915411738&hsa_grp=163076503891&hsa_ad=686577754033&hsa_src=g&hsa_tgt=kwd-11120288971&hsa_kw=timescale&hsa_mt=p&hsa_net=adwords&hsa_ver=3&gad_source=1&gclid=Cj0KCQjw-uK0BhC0ARIsANQtgGNVDQ2KKT5_xr-baYDL-aLL-qiLpHnEl23YqAjm-dM3oL3AEB8DMj8aAvIbEALw_wcB).")

# Set up tabs
hypertable_tab, compression_tab, continuous_aggregation_tab, data_retention_tab, pgvectorscale_tab = st.tabs(["Hypertable", "Compression", "Continuous Aggregation", "Data Retention", "Pgvectorscale"])

with hypertable_tab:
    st.info(
    "Querying 10M rides table from NYC cab dataset",
    icon="✍️",
    )
    
    st.info(
    "SELECT Query: SQL: SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;",
    icon="✍️",
    )
    
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
            st.subheader("Hypertable - {0:4.1f} sec".format ((hypertable_end_time - hypertable_start_time)))  
            st.dataframe(df.set_index(df.columns[0]))

            st.info(
            "To convert standard table into hypertable partitioned on the time column, run the following: SELECT create_hypertable('rides', by_range('pickup_datetime'), create_default_indexes=>FALSE);",
            icon="✍️",
            )

            st.info(
            "To setup Additional partitioning dimension to speed up queries, run the following: SELECT add_dimension('rides', by_hash('payment_type', 2));",
            icon="✍️",
            )

        with postgresql:
            st.subheader("PostgreSQL - {0:4.1f} sec".format ((postgresql_end_time - postgresql_start_time))) 
            st.dataframe(df_pg.set_index(df_pg.columns[0]))
            
            st.info(
            "Create a PostgreSQL table: CREATE TABLE  rides (vendor_id TEXT, pickup_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, dropoff_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, passenger_count NUMERIC, trip_distance NUMERIC, pickup_longitude NUMERIC, pickup_latitude NUMERIC, rate_code INTEGER, dropoff_longitude NUMERIC, dropoff_latitude  NUMERIC, payment_type INTEGER, fare_amount NUMERIC, extra NUMERIC, mta_tax NUMERIC, tip_amount NUMERIC, tolls_amount NUMERIC, improvement_surcharge NUMERIC, total_amount NUMERIC); ",
            icon="✍️",
            )

with compression_tab:
    st.info(
    "When using compression, data segmenting is based on the way you access the data eg GROUP BY"
    "With ordering, rows that change over a dimension should be close to each other."
    "By ordering the records over time, they will be compressed and accessed in the same order.",
    icon="✍️"
    )
    
    st.info(
    "To add compression to a table, run the following command: "
    "ALTER TABLE rides SET (timescaledb.compress, timescaledb.compress_segmentby='vendor_id', timescaledb.compress_orderby='pickup_datetime DESC');",
    icon="✍️",
    )

    st.info(
    "Compression policy allows to compress data older than a particular age. To set a policy run the following command: "
    "SELECT add_compression_policy('rides', INTERVAL '1 day');",
    icon="✍️",
    )

#  df_compression = conn.query('SELECT pg_size_pretty(before_compression_total_bytes) as before, pg_size_pretty(after_compression_total_bytes) as after FROM hypertable_compression_stats('rides');', ttl="0")
#    st.dataframe(df_compression.set_index(df.columns[0]))

    import pandas as pd
    import psycopg2
    from psycopg2 import sql

    # Function to execute the query and return a DataFrame
    def query_database():
        conn = psycopg2.connect(
            dbname=st.secrets["postgres"]["dbname"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
            host=st.secrets["postgres"]["host"],
            port=st.secrets["postgres"]["port"]
        )
    
        query = sql.SQL("""
        SELECT 
            pg_size_pretty(before_compression_total_bytes) as before, 
            pg_size_pretty(after_compression_total_bytes) as after 
        FROM hypertable_compression_stats('rides');
        """)
    
        df = pd.read_sql_query(query, conn)
        conn.close()
    
        return df

    # Streamlit app
    st.title("Compression Stats")

    df_compression = query_database()
    st.write(df_compression)





with continuous_aggregation_tab:
    st.write("WIP")

with data_retention_tab:
    st.write("WIP")
    
with pgvectorscale_tab:
    st.write("WIP")