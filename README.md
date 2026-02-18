## Maintenance Services Fraud & Anomaly Detection App

This Databricks App analyzes the `gold.fact.maintenanceservices` table to detect **unusual behavior and potential fraud** in maintenance service transactions.

It looks for anomalous daily transaction patterns by `orderservice` based on the `transaction_timestamp`, highlighting dates where the volume deviates significantly from each service’s historical behavior.

---

## Features

- **Spark-powered backend**: Reads from `gold.fact.maintenanceservices` using the Databricks runtime Spark session.
- **Per-service anomaly detection**:
  - Aggregates transactions per `(orderservice, transaction_date)`.
  - Computes historical mean and standard deviation of daily counts.
  - Flags days with outlier z-scores as anomalous.
- **Interactive Streamlit UI**:
  - Date range filter based on `transaction_date` derived from `transaction_timestamp`.
  - Sensitivity controls (z-score threshold, minimum days of history, minimum daily count).
  - Optional filter on specific `orderservice` values.
  - Metrics including:
    - Total transactions in range.
    - Number of anomalous `(orderservice, date)` pairs.
    - Share of transactions that occur on anomalous days.
  - Table of top anomalous service-days.
  - Optional Altair chart showing daily counts with anomalies highlighted.

---

## Project structure

- `app.py` – Streamlit Databricks App entrypoint and UI logic.
- `app.yaml` – App runtime configuration (`streamlit run app.py`).
- `requirements.txt` – Python dependencies for the app environment.

---

## Prerequisites

- A Databricks workspace with:
  - Access to the `gold.fact.maintenanceservices` table.
  - A cluster or compute that can read that table.
- Python environment that can install the `requirements.txt` packages if you want to run locally.

The table must include at least:

- `transaction_timestamp` – Timestamp of the transaction (parsable to a timestamp).
- `orderservice` – Identifier for the maintenance service order / transaction.

---

## Running in Databricks as an App

1. **Push this repo** (including `app.py`, `app.yaml`, and `requirements.txt`) to your Git provider if it is not already there.
2. In Databricks, create a **new App** and point it to this repository/path.
3. Attach a **cluster / compute** that has access to `gold.fact.maintenanceservices`.
4. Start the app from the Databricks UI.

Once the app is running:

1. Select the **transaction date range** in the sidebar.
2. Adjust:
   - Z-score threshold (anomaly sensitivity).
   - Minimum days of history per `orderservice`.
   - Minimum daily transaction count.
3. Optionally restrict to particular `orderservice` values.
4. Inspect:
   - Metrics at the top.
   - The anomalous `(orderservice, date)` table.
   - The chart of daily counts with anomalies highlighted (if Altair is available).

---

## Running locally (optional)

You can also run the app locally for development, provided you have a Spark environment with access to the same table or a test table with the same schema.

1. Create and activate a virtual environment (optional but recommended).
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Ensure your environment can access a Spark session and the `gold.fact.maintenanceservices` table (or a local equivalent).
4. Run:

   ```bash
   streamlit run app.py
   ```

Note: In Databricks Apps, Spark is provided via `databricks.sdk.runtime`. Locally, `app.py` will fall back to `SparkSession.builder.getOrCreate()`, so you must have Spark configured on your machine.

---

## Next steps / customization

- Extend the anomaly logic to include:
  - Transaction amounts.
  - Customer segments.
  - Channels or payment methods.
- Add drill-down views to list individual transactions for a selected anomalous `(orderservice, date)`.
- Log anomalies to a dedicated table for audit and alerting workflows.

