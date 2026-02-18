import streamlit as st

try:
    # In Databricks Apps, this provides a configured SparkSession named `spark`
    from databricks.sdk.runtime import *
except ImportError:
    # Fallback for local development
    from pyspark.sql import SparkSession  # type: ignore
    spark = SparkSession.builder.getOrCreate()  # type: ignore

from pyspark.sql import functions as F  # type: ignore
from pyspark.sql import Window  # type: ignore


@st.cache_resource(show_spinner=False)
def load_base_dataframe():
    """
    Load the transactions table from the lakehouse and add a transaction_date column.
    """
    df = spark.table("gold.fact.maintenanceservices")

    # Ensure we have a proper timestamp and date column
    if "transaction_timestamp" not in df.columns:
        raise ValueError("Column 'transaction_timestamp' not found in gold.fact.maintenanceservices.")
    if "orderservice" not in df.columns:
        raise ValueError("Column 'orderservice' not found in gold.fact.maintenanceservices.")

    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp("transaction_timestamp"),
    ).withColumn(
        "transaction_date",
        F.to_date("transaction_timestamp"),
    )

    return df


def compute_anomalies(
    df,
    z_threshold: float = 3.0,
    min_days_per_orderservice: int = 10,
    min_txn_count: int = 1,
):
    """
    Simple unsupervised anomaly detection based on how unusual the daily
    transaction count is for each orderservice.

    For each (orderservice, transaction_date), we compute:
      - daily transaction count
      - mean and stddev of that count across all days for that orderservice
      - z-score for that day
    We then flag rows whose |z-score| exceeds the configured threshold.
    """
    daily = (
        df.groupBy("orderservice", "transaction_date")
        .agg(F.count(F.lit(1)).alias("txn_count"))
    )

    w = Window.partitionBy("orderservice")

    stats = (
        daily.withColumn("mean_count", F.avg("txn_count").over(w))
        .withColumn("std_count", F.stddev_pop("txn_count").over(w))
        .withColumn("days_observed", F.count(F.lit(1)).over(w))
    )

    stats = stats.withColumn(
        "z_score",
        F.when(F.col("std_count") == 0, F.lit(0.0)).otherwise(
            (F.col("txn_count") - F.col("mean_count")) / F.col("std_count")
        ),
    )

    anomalies = stats.filter(
        (F.col("days_observed") >= F.lit(min_days_per_orderservice))
        & (F.col("txn_count") >= F.lit(min_txn_count))
        & (F.abs(F.col("z_score")) >= F.lit(z_threshold))
    )

    return anomalies


def main():
    st.set_page_config(
        page_title="Fraud & Anomaly Detection - Maintenance Services",
        layout="wide",
    )

    st.title("Fraud & Unusual Behavior Detection")
    st.caption(
        "Monitoring `gold.fact.maintenanceservices` for anomalous transaction patterns "
        "by **orderservice** and **transaction_timestamp**."
    )

    with st.spinner("Loading transactions from gold.fact.maintenanceservices..."):
        try:
            base_df = load_base_dataframe()
        except Exception as e:
            st.error(f"Failed to load table `gold.fact.maintenanceservices`: {e}")
            return

    date_bounds = (
        base_df.select(
            F.min("transaction_date").alias("min_date"),
            F.max("transaction_date").alias("max_date"),
        )
        .first()
    )

    if not date_bounds or date_bounds["min_date"] is None or date_bounds["max_date"] is None:
        st.warning("No transaction dates found in the source table.")
        return

    min_date = date_bounds["min_date"]
    max_date = date_bounds["max_date"]

    with st.sidebar:
        st.header("Filters & Thresholds")

        date_range = st.date_input(
            "Transaction date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(date_range, tuple):
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date, max_date

        z_threshold = st.slider(
            "Anomaly sensitivity (z-score threshold)",
            min_value=1.0,
            max_value=5.0,
            value=3.0,
            step=0.5,
            help="Lower values flag more transactions as unusual.",
        )

        min_days = st.slider(
            "Minimum days observed per orderservice",
            min_value=5,
            max_value=90,
            value=10,
            step=1,
        )

        min_txn_count = st.slider(
            "Minimum daily transaction count",
            min_value=1,
            max_value=100,
            value=1,
            step=1,
        )

    filtered_df = base_df.filter(
        (F.col("transaction_date") >= F.lit(start_date))
        & (F.col("transaction_date") <= F.lit(end_date))
    )

    orderservices = (
        filtered_df.select("orderservice")
        .distinct()
        .orderBy("orderservice")
        .limit(5000)
    )

    orderservice_list = [r["orderservice"] for r in orderservices.collect()]

    selected_orderservices = st.multiselect(
        "Restrict to specific orderservice values (optional)",
        options=orderservice_list,
    )

    if selected_orderservices:
        filtered_df = filtered_df.filter(F.col("orderservice").isin(selected_orderservices))

    with st.spinner("Computing anomalies..."):
        anomalies_df = compute_anomalies(
            filtered_df,
            z_threshold=z_threshold,
            min_days_per_orderservice=min_days,
            min_txn_count=min_txn_count,
        )

    total_txns = filtered_df.count()
    anomaly_count = anomalies_df.count()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total transactions in range", f"{total_txns:,}")
    with col2:
            st.metric("Flagged anomalous days", f"{anomaly_count:,}")
    with col3:
        frac = (anomaly_count / total_txns) if total_txns > 0 else 0
        st.metric("Anomalous share (approx.)", f"{100 * frac:.2f}%")

    if anomaly_count == 0:
        st.success("No anomalous transaction patterns detected with the current settings.")
        return

    # Show the top anomalous patterns ordered by z-score magnitude
    top_anomalies = (
        anomalies_df.orderBy(F.desc(F.abs(F.col("z_score"))))
        .limit(1000)
        .toPandas()
    )

    st.subheader("Anomalous transaction patterns by orderservice and day")
    st.write(
        "Each row represents an `(orderservice, transaction_date)` where the daily "
        "transaction volume deviates significantly from that service's historical behavior."
    )
    st.dataframe(
        top_anomalies,
        use_container_width=True,
        hide_index=True,
    )

    # Optional simple visualization
    try:
        import altair as alt  # type: ignore

        st.subheader("Daily transaction counts with anomalies highlighted")

        chart_data = top_anomalies.copy()
        chart_data["is_anomaly"] = True

        base_chart = alt.Chart(chart_data).mark_bar().encode(
            x="transaction_date:T",
            y="txn_count:Q",
            color=alt.condition(
                alt.datum.is_anomaly,
                alt.value("red"),
                alt.value("steelblue"),
            ),
            tooltip=[
                "orderservice",
                "transaction_date",
                "txn_count",
                "mean_count",
                "std_count",
                "z_score",
            ],
        )

        st.altair_chart(base_chart, use_container_width=True)
    except Exception:
        # Altair is optional; if not available we just skip the chart.
        pass


if __name__ == "__main__":
    main()

