import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import time

st.title("TimescaleDB vs PostgreSQL")
st.write("Try Timescale for Free [Link](https://www.timescale.com/?utm_source=google&utm_medium=cpc&utm_campaign=brand&utm_term=timescale&utm_campaign=g_search_brand&utm_source=adwords&utm_medium=ppc&hsa_acc=9771591554&hsa_cam=20915411738&hsa_grp=163076503891&hsa_ad=686577754033&hsa_src=g&hsa_tgt=kwd-11120288971&hsa_kw=timescale&hsa_mt=p&hsa_net=adwords&hsa_ver=3&gad_source=1&gclid=Cj0KCQjw-uK0BhC0ARIsANQtgGNVDQ2KKT5_xr-baYDL-aLL-qiLpHnEl23YqAjm-dM3oL3AEB8DMj8aAvIbEALw_wcB).")

# Set up tabs
hypertable_tab, compression_tab, continuous_aggregation_tab, data_retention_tab, data_tiering_tab, pgvectorscale_tab = st.tabs(["Hypertable", "Compression", "Continuous Aggregation", "Data Retention", "Data Tiering", "Pgvectorscale"])

with hypertable_tab:
    st.info(
    "Querying 10M rides table from NYC cab dataset",
    icon="✍️",
    )
    
    st.info(
    "SELECT Query: ",
    icon="✍️",
    )

    st.success(
    "SELECT rate_code, COUNT(vendor_id) AS num_trips FROM rides GROUP BY rate_code ORDER BY rate_code;"
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
            "To convert standard table into hypertable partitioned on the time column, run the following: ",
            icon="✍️",
            )

            st.success(
            "SELECT create_hypertable('rides', by_range('pickup_datetime'), create_default_indexes=>FALSE);"
            )

            st.info(
            "To setup additional partitioning dimensions to speed up queries (eg for GROUP BY ops), run the following: ",
            icon="✍️",
            )

            st.success(
            "SELECT add_dimension('rides', by_hash('payment_type', 2));"    
            )

            st.success(
            "SELECT add_dimension('rides', by_hash('rate_code', 2));"
            )

        with postgresql:
            st.subheader("PostgreSQL - {0:4.1f} sec".format ((postgresql_end_time - postgresql_start_time))) 
            st.dataframe(df_pg.set_index(df_pg.columns[0]))
            
            st.info(
            "Create a PostgreSQL table: ",
            icon="✍️",
            )

            st.success(
            "CREATE TABLE  rides (vendor_id TEXT, pickup_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, dropoff_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, passenger_count NUMERIC, trip_distance NUMERIC, pickup_longitude NUMERIC, pickup_latitude NUMERIC, rate_code INTEGER, dropoff_longitude NUMERIC, dropoff_latitude  NUMERIC, payment_type INTEGER, fare_amount NUMERIC, extra NUMERIC, mta_tax NUMERIC, tip_amount NUMERIC, tolls_amount NUMERIC, improvement_surcharge NUMERIC, total_amount NUMERIC);"
            )

with compression_tab:
    st.info(
    "To add compression to a table, run the following command: ",
    icon="✍️",
    )

    st.success(
    "ALTER TABLE rides \n\nSET (timescaledb.compress, timescaledb.compress_segmentby='rate_code, payment_type', timescaledb.compress_orderby='pickup_datetime DESC');"    
    )
    
    query = "SELECT pg_size_pretty(before_compression_total_bytes) as Total_Bytes_Before_Compression, pg_size_pretty(after_compression_total_bytes) as Total_Bytes_After_Compression, round(before_compression_total_bytes / after_compression_total_bytes::numeric, 2) as Compression_Ratio, round(1- after_compression_total_bytes / before_compression_total_bytes::numeric , 3) * 100 as Compression_Pct FROM hypertable_compression_stats('rides');"
    df_compression = conn.query(query, ttl="0")
    st.dataframe(df_compression.set_index(df_compression.columns[0]))

    st.info(
    "When using compression, data segmenting is based on the way you access the data eg GROUP BY "
    "With ordering, rows that change over a dimension should be close to each other. "
    "By ordering the records over time, they will be compressed and accessed in the same order. ",
    icon="✍️"
    ) 

    st.info(
    "Compression policy enables automated compression of older than a particular age. To set a policy run the following command: ",
    icon="✍️",
    )

    st.success(
    "SELECT add_compression_policy('rides', INTERVAL '1 day');"    
    )


with continuous_aggregation_tab:
    st.info("Continuous aggregates are designed to make queries on very large datasets run faster. Timescale continuous aggregates use PostgreSQL materialized views to continuously and incrementally refresh a query in the background, so that when you run the query, only the data that has changed needs to be computed, not the entire dataset." ,
    icon="✍️"
    )
    
    st.info("To create & query continuous aggregate: ",
    icon="✍️"
    )

    st.success(
    "CREATE MATERIALIZED VIEW ride_stats_by_hour WITH (timescaledb.continuous) AS SELECT time_bucket('60 minute', pickup_datetime) AS interval, count(*) as num_trips, round(avg(fare_amount),2) as avg_fare, avg(dropoff_datetime - pickup_datetime) as avg_trip_duration, round(avg(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)))/60,2) as avg_trip_duration_min FROM rides WHERE pickup_datetime < '2016-01-08 00:00' GROUP BY interval;"
    )

    # Run base table query
    query = "SELECT time_bucket('60 minute', pickup_datetime) AS interval, count(*) as num_trips FROM rides WHERE pickup_datetime < '2016-01-08 00:00' GROUP BY interval ORDER BY interval ASC;"
    base_table_start_time = time.time()
    df_base_table = conn.query(query, ttl="0")
    base_table_end_time = time.time()

    # Display base table results
    st.subheader("Querying raw data - {0:4.2f} sec".format ((base_table_end_time - base_table_start_time)))  
    chart_data = pd.DataFrame(df_base_table, columns=["num_trips"])
    st.bar_chart(chart_data)

    # Run materialized_view query
    query = "SELECT interval, num_trips FROM ride_stats_by_hour ORDER BY interval ASC;"
    mv_start_time = time.time()
    df_mv = conn.query(query, ttl="0")
    mv_end_time = time.time()

    # Display materialized_view results
    st.subheader("Querying Materialized View - {0:4.2f} sec".format ((mv_end_time - mv_start_time)))  
    chart_data = pd.DataFrame(df_mv, columns=["num_trips"])
    st.bar_chart(chart_data)
    
    st.info("Add a refresh policy to keep the continuous aggregate up-to-date: ",
    icon="✍️"
    ) 

    st.success(
    "SELECT add_continuous_aggregate_policy ('ride_stats_by_hour', start_offset => NULL, end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour');"
    )

with data_retention_tab:
    st.write("WIP")

with data_tiering_tab:
    st.write("WIP")
    
with pgvectorscale_tab:
    st.write("WIP")