# Governance Agent Demo

This repository contains a demo of a Governance Agent for a Retailer of Clothes, Home, and Fashion accessories in Europe.

## Components

### 1. Data Generation (`/data_generation`)
Generates synthetic BigQuery data for Customers, Products, Orders, and Transactions.

### 2. Dataplex Integration (`/dataplex_integration`)
Associates Dataplex Catalog metadata with the BigQuery dataset and exports it back to BigQuery for the agent to use.

### 3. Governance Agent (`/governance_agent`)
A React-based agent using the ADK toolkit to provide governance insights.

## Getting Started

### Prerequisites
- Google Cloud Project with the following APIs enabled:
  - BigQuery API (`bigquery.googleapis.com`)
  - Dataplex API (`dataplex.googleapis.com`)
  - Cloud Storage API (`storage.googleapis.com`)
  - Data Catalog API (`datacatalog.googleapis.com`)
  - Vertex AI API (`aiplatform.googleapis.com`)
  - Cloud AI Companion API (`cloudaicompanion.googleapis.com`)
- Python 3.8+
- Node.js & npm

### Setup

1. **Data Generation**:
   ```bash
   cd data_generation
   pip install -r requirements.txt
   export GOOGLE_CLOUD_PROJECT=your-project-id
   python generate_data.py
   ```

2. **Dataplex Integration**:
   ```bash
   cd ../dataplex_integration
   pip install -r requirements.txt
   python associate_aspects.py
   python manage_scans.py
   python manage_insights.py
   python export_metadata.py
   ```

3. **Governance Agent**:
   See `governance_agent/README.md` for instructions.
