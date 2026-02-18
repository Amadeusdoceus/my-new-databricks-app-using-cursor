import streamlit as st

import os
import re
from pathlib import Path

from pyspark.sql import functions as F  # type: ignore
from pyspark.sql import Window  # type: ignore


@st.cache_resource(show_spinner=False)
def bootstrap_databricks_auth_from_scripts():
    """
    Best-effort auth bootstrap for local development.

    If the repository includes `scripts/scripts/geraToken`, we extract HOST/PROFILE from it
    and use Databricks SDK with `auth_type="databricks-cli"` to obtain a token. We then set:
      - DATABRICKS_HOST
      - DATABRICKS_TOKEN
      - DATABRICKS_CONFIG_PROFILE

    In Databricks Apps / Databricks compute this is usually unnecessary, but it helps when
    running locally with an existing CLI profile.
    """
    if os.getenv("DATABRICKS_TOKEN") and os.getenv("DATABRICKS_HOST"):
        return

    base_dir = Path(__file__).resolve().parent
    script_candidates = [
        Path(r"C:\Databricks\scripts\scripts\geraToken"),
        base_dir / "scripts" / "scripts" / "geraToken",
        base_dir.parent / "scripts" / "scripts" / "geraToken",
    ]

    script_path = next((p for p in script_candidates if p.exists()), None)
    if script_path is None:
        return

    try:
        content = script_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return

    host_match = re.search(r'^\s*HOST\s*=\s*"([^"]+)"\s*$', content, flags=re.MULTILINE)
    profile_match = re.search(r'^\s*PROFILE\s*=\s*"([^"]+)"\s*$', content, flags=re.MULTILINE)
    if not host_match or not profile_match:
        return

    host = host_match.group(1).strip()
    profile = profile_match.group(1).strip()
    if not host or not profile:
        return

    # Keep environment variables consistent with the host/profile used to mint the token.
    # Using setdefault here can leave a pre-set DATABRICKS_HOST pointing to a different
    # workspace than the token we generate below.
    os.environ["DATABRICKS_HOST"] = host
    os.environ["DATABRICKS_CONFIG_PROFILE"] = profile

    try:
        from databricks.sdk import WorkspaceClient  # type: ignore

        w = WorkspaceClient(host=host, profile=profile, auth_type="databricks-cli")
        headers = w.config.authenticate()
        if callable(headers):
            headers = headers()
        token = headers["Authorization"].replace("Bearer ", "")
        if token:
            os.environ["DATABRICKS_TOKEN"] = token
    except Exception:
        # If CLI auth isn't available in this environment, we simply continue.
        return


@st.cache_resource(show_spinner=False)
def get_spark_session():
    bootstrap_databricks_auth_from_scripts()

    # Prefer an injected Spark session when running on Databricks
    try:
        from databricks.sdk.runtime import spark as db_spark  # type: ignore

        if db_spark is not None:
            st.sidebar.success("✅ Using Databricks runtime Spark session")
            return db_spark
    except Exception as e:
        st.sidebar.warning(f"⚠️ Databricks runtime Spark not available: {e}")

    # Optional: Databricks Connect (local -> remote)
    try:
        from databricks.connect import DatabricksSession  # type: ignore

        session = DatabricksSession.builder.getOrCreate()
        if session is not None:
            st.sidebar.success("✅ Using Databricks Connect session")
            return session
    except Exception as e:
        st.sidebar.warning(f"⚠️ Databricks Connect not available: {e}")

    # Fallback: local Spark
    try:
        from pyspark.sql import SparkSession  # type: ignore

        session = SparkSession.builder.getOrCreate()  # type: ignore
        if session is not None:
            st.sidebar.warning("⚠️ Using local Spark session (may not access Databricks tables)")
            return session
    except Exception as e:
        st.sidebar.error(f"❌ Failed to create Spark session: {e}")
        raise RuntimeError(f"Could not create any Spark session: {e}")

    raise RuntimeError("Failed to obtain Spark session: all methods returned None")


@st.cache_resource(show_spinner=False)
def load_base_dataframe():
    """
    Load the transactions table from the lakehouse and add a transaction_date column.
    """
    spark = get_spark_session()
    
    if spark is None:
        raise RuntimeError("Spark session is None - cannot load table")
    
    # Try both table name formats (with dot and underscore)
    table_candidates = [
        "gold.fact.maintenanceservices",  # Unity Catalog format
        "gold.fact_maintenanceservices",   # Legacy format with underscore
    ]
    
    df = None
    used_table_name = None
    
    for table_name in table_candidates:
        try:
            st.sidebar.info(f"🔍 Trying table: {table_name}")
            df = spark.table(table_name)
            used_table_name = table_name
            st.sidebar.success(f"✅ Found table: {table_name}")
            break
        except Exception as e:
            st.sidebar.warning(f"⚠️ Table {table_name} not found: {str(e)[:100]}")
            continue
    
    if df is None:
        available_tables = []
        try:
            # Try to list tables in gold.fact schema
            available_tables = spark.sql("SHOW TABLES IN gold.fact").collect()
            table_list = [row.tableName for row in available_tables]
            st.sidebar.error(f"❌ Table not found. Available tables in gold.fact: {table_list}")
        except Exception:
            st.sidebar.error("❌ Could not list available tables")
        
        raise ValueError(
            f"Table not found. Tried: {', '.join(table_candidates)}. "
            f"Please verify the table name exists in your Databricks workspace."
        )

    # Ensure we have a proper timestamp and date column
    if "transaction_timestamp" not in df.columns:
        raise ValueError(f"Column 'transaction_timestamp' not found in {used_table_name}.")
    if "orderservice" not in df.columns:
        raise ValueError(f"Column 'orderservice' not found in {used_table_name}.")

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
            st.error(f"❌ Failed to load table `gold.fact.maintenanceservices`: {e}")
            st.error(f"**Error type:** `{type(e).__name__}`")
            st.error(f"**Error details:** `{str(e)}`")
            
            # Mostra informações de debug
            with st.expander("🔍 Debug Information"):
                st.write("**Spark Session Status:**")
                try:
                    spark = get_spark_session()
                    if spark is None:
                        st.error("Spark session is None")
                    else:
                        st.success(f"Spark session type: {type(spark).__name__}")
                        try:
                            # Tenta listar databases
                            dbs = spark.sql("SHOW DATABASES").collect()
                            st.write(f"**Available databases:** {[db.databaseName for db in dbs]}")
                        except Exception as db_err:
                            st.warning(f"Could not list databases: {db_err}")
                        
                        try:
                            # Tenta listar tabelas em gold.fact
                            tables = spark.sql("SHOW TABLES IN gold.fact").collect()
                            st.write(f"**Tables in gold.fact:** {[t.tableName for t in tables]}")
                        except Exception as tbl_err:
                            st.warning(f"Could not list tables in gold.fact: {tbl_err}")
                except Exception as spark_err:
                    st.error(f"Could not get Spark session: {spark_err}")
            
            st.info("💡 **Dica:** Execute `scripts\\verify_table.bat` para verificar a tabela via Databricks CLI.")
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

    # Number of individual transactions that fall on anomalous days
    anomaly_days = anomalies_df.select("orderservice", "transaction_date").distinct()
    anomaly_txns = (
        filtered_df.join(
            anomaly_days,
            on=["orderservice", "transaction_date"],
            how="inner",
        ).count()
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total transactions in range", f"{total_txns:,}")
    with col2:
        st.metric("Flagged anomalous days", f"{anomaly_count:,}")
    with col3:
        frac = (anomaly_txns / total_txns) if total_txns > 0 else 0
        st.metric("Share of txns on anomalous days", f"{100 * frac:.2f}%")

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

        # Build baseline daily counts and join anomaly statistics so that
        # non-anomalous days are visible alongside anomalous ones.
        daily_counts_df = (
            filtered_df.groupBy("orderservice", "transaction_date")
            .agg(F.count(F.lit(1)).alias("txn_count"))
        )

        anomaly_details = anomalies_df.select(
            "orderservice",
            "transaction_date",
            "z_score",
            "mean_count",
            "std_count",
        )

        chart_df_spark = (
            daily_counts_df.join(
                anomaly_details,
                on=["orderservice", "transaction_date"],
                how="left",
            )
            .withColumn("is_anomaly", F.col("z_score").isNotNull())
        )

        # Limit the number of rows materialized on the driver to keep
        # the chart responsive for very large datasets.
        chart_data = chart_df_spark.limit(5000).toPandas()

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

