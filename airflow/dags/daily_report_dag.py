from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

RAW_PATH = "/opt/airflow/data/raw"
REPORT_PATH = "/opt/airflow/data/reports"

def generate_daily_report():
    os.makedirs(REPORT_PATH, exist_ok=True)

    df = pd.read_parquet(RAW_PATH)

    views = df[df["event_type"] == "view"]
    purchases = df[df["event_type"] == "purchase"]

    top_products = views["product_id"].value_counts().head(5)

    all_users = set(df["user_id"].unique())
    buyers = set(purchases["user_id"].unique())
    window_shoppers = all_users - buyers

    category_summary = df.groupby("category").agg(
        views=("event_type", lambda x: (x == "view").sum()),
        purchases=("event_type", lambda x: (x == "purchase").sum())
    )

    category_summary["conversion_rate_percent"] = (
        category_summary["purchases"] / category_summary["views"] * 100
    ).round(2)

    report_file = f"{REPORT_PATH}/daily_clickstream_report.txt"
    csv_file = f"{REPORT_PATH}/conversion_rate_report.csv"

    category_summary.to_csv(csv_file)

    with open(report_file, "w") as f:
        f.write("Daily E-Commerce Clickstream Report\n")
        f.write("===================================\n\n")

        f.write("Top 5 Most Viewed Products\n")
        f.write("--------------------------\n")
        f.write(str(top_products))
        f.write("\n\n")

        f.write("User Segmentation\n")
        f.write("-----------------\n")
        f.write(f"Total Buyers: {len(buyers)}\n")
        f.write(f"Total Window Shoppers: {len(window_shoppers)}\n\n")

        f.write("Conversion Rate by Category\n")
        f.write("---------------------------\n")
        f.write(str(category_summary))
        f.write("\n")

default_args = {
    "owner": "student",
    "start_date": datetime(2026, 5, 9)
}

with DAG(
    dag_id="daily_ecommerce_clickstream_report",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    generate_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report
    )

    generate_report