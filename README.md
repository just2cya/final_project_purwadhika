E-Commerce Order Pipeline: Hybrid Streaming & Batch Processing

📌 Project Overview
In fast-paced e-commerce and distribution environments, data is high-volume, fragmented, and time-sensitive. This project demonstrates a production-grade data engineering architecture designed to handle two critical business needs:
- Real-time Action: Identifying critical events (Fraud Detection) as they happen.
- Strategic Analysis: Preparing historical data (Batch ETL) for long-term reporting and business intelligence.
By leveraging a modern data stack, this pipeline transforms raw application data into a clean, star-schema-based data mart, enabling stakeholders to make data-driven decisions with confidence.

🏗️ Architecture
The system is built on a hybrid architecture to support both real-time and analytical workloads:
Infrastructure: Hosted on Google Compute Engine (GCE) using Docker for containerized services.
Stream Layer: Orders are ingested via Python and buffered through Google Cloud Pub/Sub for immediate fraud detection analysis.
Batch Layer: Historical data is processed via Apache Airflow, transforming raw data into structured formats.
Transformation: dbt (Data Build Tool) handles the modeling logic, creating a robust Star Schema and specialized Data Marts.
Storage: PostgreSQL (staging) and Google BigQuery (warehouse).
Monitoring: Integrated Telegram Bot API for real-time pipeline status and error alerting.

👨‍💻 About the Author
I am a Data Enthusiast with over 10 years of experience in the distribution sector.
My journey from Customer Support to Fullstack Developer and now Data Engineering has given me a "Full-Circle" perspective on data—understanding not just how to move it, but why it matters to the business.
Developed as part of a Data Engineering Specialization Capstone Project.
