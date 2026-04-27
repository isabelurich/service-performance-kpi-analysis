# 📊 Service Performance & Commercial KPI Analysis

## Overview
This project presents an end-to-end data pipeline designed to evaluate customer service performance and commercial outcomes.

It transforms raw operational data into structured, analytics-ready datasets, enabling performance tracking at the agent level and supporting data-driven decision-making.

---

## Business Context
Customer service agents are evaluated based on operational efficiency and commercial performance. Key challenges include:

- Data distributed across multiple systems
- Inconsistent customer records
- Limited visibility into individual performance
- Difficulty tracking KPIs over time

---

## Solution
A multi-layered data pipeline was developed to integrate, clean, and enrich service and commercial data.

The solution includes:
- Data extraction from raw operational sources
- Integration with customer and organizational datasets
- Data quality validation and feature engineering
- KPI aggregation and performance classification

---

## Data Architecture

The project follows a layered data model:

- **Raw Layer** → Source operational data  
- **Trusted Layer** → Cleaned and standardized datasets  
- **Analytics Layer** → Aggregated KPIs and business metrics  

---

## Key Features

### Service Data Pipeline
- Cleans and structures customer service records
- Standardizes fields and filters invalid data

### 🔹 Customer Data Quality KPI
- Calculates a **data completeness score**
- Evaluates presence of key attributes:
  - Name
  - Document
  - Contact information
  - Address data
- Includes **region-specific logic** for data sourcing

### 🔹 Agent Performance Analysis
- Aggregates KPIs at agent level
- Calculates:
  - Total services
  - % of complete registrations
- Assigns **performance tiers** based on business thresholds

### 🔹 Financial Agreements Analysis
- Processes installment plan data
- Calculates:
  - Discount amounts
  - Down payment values
- Segments agents by operational role

---

## Key Metrics
- Number of customer interactions
- Customer data completeness score
- % of fully completed registrations
- Agent performance tier
- Average down payment rate
- Discount applied on financial agreements

---

## Technologies Used
- PySpark
- Spark SQL
- Data Lake architecture
- SQL-based transformations
- Data modeling for analytics

---

## Data Pipeline Flow

1. Extract raw service and financial data  
2. Join with reference and customer datasets  
3. Apply business rules and data quality logic  
4. Generate analytical datasets  
5. Aggregate KPIs at agent level  

---

## Disclaimer
All data structures, names, and fields have been anonymized to preserve confidentiality.  
This project is intended for portfolio purposes only.
