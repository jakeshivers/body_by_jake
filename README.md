
# 💪 Body By Jake — Big Data Fitness Analytics Platform

##🎯 Objective
Build a full-stack data lakehouse project that simulates data ingestion from a multi-service gym operation. Apply the medallion architecture (Bronze → Silver → Gold) using PySpark on Databricks, and prepare for future ML integration such as churn prediction, class popularity forecasting, or customer segmentation.

## 📦 Data Domains (Synthetic + Modular)
Generate and process realistic gym data across multiple verticals:

## Data Source	Description
🧑‍💼 Memberships	Join date, billing cycle, status, plan (basic, elite, etc.) <br>
🧘 Class Check-ins	Spinning, Yoga, Stretch, Peleton, timestamps, instructor <br>
🏊 Facility Usage	Sauna, pool, duration <br>
🥤 Retail Store	Purchases from juice bar, supplements, snacks <br>
💬 App Usage	Class bookings, calendar usage, goal tracking <br>
🚫 Cancellations	Timestamps, reasons, history of usage <br>
🏋️ Equipment Usage	Machines used, reps logged, time per session <br>

## 🧱 Architecture Overview
### Bronze Layer: Raw data in Parquet format, partitioned by date <br>
- Ingested via PySpark scripts on Databricks
- Simulate real-time ingestion using Kafka or Auto Loader

### Silver Layer: Cleaned and normalized tables <br>
- Deduplicate class check-ins
- Normalize purchase SKUs
- Parse timestamp fields into proper formats

### Gold Layer: Business-ready aggregates <br>
- Daily active users
- Revenue by product/class
- Customer LTV
- Churn cohort analysis

## 🛠️ Tech Stack <br>

| Layer         | Tools                                                                 |
|---------------|-----------------------------------------------------------------------|
| Ingestion     | PySpark (streaming or batch), Databricks Auto Loader                 |
| Storage       | Delta Lake (on S3 or DBFS)                                            |
| Transform     | PySpark + Databricks Notebooks                                        |
| Model Orchestration | Databricks Workflows, or optionally Airflow                    |
| ML Prep       | MLflow for model tracking, scikit-learn or Spark MLlib               |
| Infra         | Docker for local dev, Kubernetes for scalable ingestion jobs         |
| CI/CD         | GitHub Actions for deployment of ETL pipelines                       |
| Observability | Great Expectations or Soda Core for validation                       |


## 🐳 Docker/Kubernetes Setup <br>
- Docker to simulate local ingestion of data (e.g., REST API → Kafka → Spark job).
- Use Docker Compose to run:
- Kafka
- Jupyter/PySpark container
- Set up Minikube or Kind to run:
- A Spark cluster on K8s (with Spark Operator)
- Simulated ingestion pipelines

## 📊 ML/Analytics Add-On (Post-Core Build) <br>
- Predict churn risk using logistic regression on class attendance + purchase data
- Recommend classes or products using collaborative filtering
- Forecast weekly supplement sales with time series modeling (Prophet or Spark ML)

## 📝 Deliverables <br>
- README.md with architecture diagram and walkthrough
- Terraform or shell script to set up cloud infra (if using AWS S3 + Databricks)
- Docker Compose file or K8s manifest for local simulation
- PySpark scripts organized in /bronze, /silver, /gold
- Notebooks for EDA and ML in /notebooks
- .yml workflows for GitHub Actions
- Synthetic data generator in /data_gen using Faker

## 💥 Why I'm doing this project <br>
- Combines big data ingestion + structured transformations + ML readiness
- Uses open-source tools and cloud-native patterns
- Simulates real business use cases (churn, LTV, DAUs)
- Shows off orchestration, data modeling, and system design