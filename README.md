# Serverless ETL Pipeline 🚀

Transforms raw CSV data into business intelligence using Python, Pandas and Claude AI.

## The Problem
Raw sales data sitting in CSV files tells you nothing. You need transformation, aggregation and interpretation to make business decisions.

## What It Does
- Extracts raw CSV data (sales transactions)
- Transforms using Pandas — revenue calculation, category and region grouping
- Analyzes with Claude AI for business insights
- Loads results to clean output CSV files and JSON report

## Sample Results
Total Revenue Processed: $27,299.43
Total Transactions: 20

Key Insights from Claude:
- Electronics = 81.3% of total revenue
- North region outperforms all others at 42.5% of revenue
- Phones are the single biggest revenue driver at 39.6%
- East region underperforming — opportunity for expansion

## Tech Stack
- Python 3
- Pandas
- Claude API (Anthropic)
- AWS S3 (for production deployment)
- AWS Lambda (serverless execution)

## Key Concepts Demonstrated
- ETL pipeline design
- Data transformation and aggregation
- AI powered business intelligence
- AWS Well-Architected Performance Pillar

## How To Run
- Clone the repo
- pip install pandas anthropic python-dotenv
- Add your ANTHROPIC_API_KEY to .env
- Run py etl.py

## Part of my 30 cloud projects in 30 days series
Follow along: https://www.linkedin.com/in/aishatolatunji/