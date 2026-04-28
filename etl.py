import os
import json
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import anthropic

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

def extract(filepath):
    print(f"📥 Extracting data from {filepath}...")
    df = pd.read_csv(filepath)
    print(f"Extracted {len(df)} rows and {len(df.columns)} columns\n")
    return df

def transform(df):
    print("🔄 Transforming data...")
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = df['quantity'] * df['price']
    df['month'] = df['date'].dt.strftime('%Y-%m')

    category_summary = df.groupby('category').agg(
        total_revenue=('revenue', 'sum'),
        total_quantity=('quantity', 'sum'),
        avg_price=('price', 'mean'),
        num_transactions=('date', 'count')
    ).reset_index()

    region_summary = df.groupby('region').agg(
        total_revenue=('revenue', 'sum'),
        total_quantity=('quantity', 'sum')
    ).reset_index()

    product_summary = df.groupby('product').agg(
        total_revenue=('revenue', 'sum'),
        total_quantity=('quantity', 'sum')
    ).reset_index().sort_values('total_revenue', ascending=False)

    print(f"✅ Transformed {len(df)} rows successfully\n")

    return {
        'raw_data': df,
        'category_summary': category_summary,
        'region_summary': region_summary,
        'product_summary': product_summary,
        'total_revenue': df['revenue'].sum(),
        'total_transactions': len(df)
    }

def analyze_with_claude(transformed_data):
    print("🤖 Sending to Claude AI for business insights...")

    summary = {
        'total_revenue': float(transformed_data['total_revenue']),
        'total_transactions': transformed_data['total_transactions'],
        'category_summary': transformed_data['category_summary'].to_dict('records'),
        'region_summary': transformed_data['region_summary'].to_dict('records'),
        'product_summary': transformed_data['product_summary'].to_dict('records')
    }

    for attempt in range(3):
        try:
            message = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=400,
                messages=[{
                    "role": "user",
                    "content": f"""
You are a business intelligence analyst. Analyze this sales data and provide insights.

DATA SUMMARY:
{json.dumps(summary, indent=2)}

Provide:
1. Top 3 business insights from this data
2. Best performing category and region
3. Revenue optimization recommendations
4. Any concerning trends to watch

Keep response under 250 words. Be specific with numbers.
                    """
                }]
            )
            return message.content[0].text
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                print("Retrying in 5 seconds...")
                time.sleep(5)

    return "Claude analysis unavailable — API temporarily down. ETL completed successfully."

def load(transformed_data, analysis):
    print("📤 Loading results...")
    transformed_data['category_summary'].to_csv('output_category.csv', index=False)
    transformed_data['region_summary'].to_csv('output_region.csv', index=False)
    transformed_data['product_summary'].to_csv('output_products.csv', index=False)

    report = {
        'timestamp': datetime.now().isoformat(),
        'total_revenue': float(transformed_data['total_revenue']),
        'total_transactions': transformed_data['total_transactions'],
        'category_summary': transformed_data['category_summary'].to_dict('records'),
        'region_summary': transformed_data['region_summary'].to_dict('records'),
        'product_summary': transformed_data['product_summary'].to_dict('records'),
        'claude_insights': analysis
    }

    with open('etl_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    print("✅ Output files saved:")
    print("   - output_category.csv")
    print("   - output_region.csv")
    print("   - output_products.csv")
    print("   - etl_report.json\n")

def run():
    print("🚀 Serverless ETL Pipeline")
    print("==========================\n")

    df = extract('./data/sales_data.csv')
    transformed = transform(df)

    print("📊 TRANSFORMATION RESULTS:")
    print(f"Total Revenue: ${transformed['total_revenue']:,.2f}")
    print(f"Total Transactions: {transformed['total_transactions']}")
    print("\nBy Category:")
    print(transformed['category_summary'].to_string(index=False))
    print("\nBy Region:")
    print(transformed['region_summary'].to_string(index=False))

    analysis = analyze_with_claude(transformed)
    print("\n=============================")
    print("🤖 CLAUDE BUSINESS INSIGHTS")
    print("=============================\n")
    print(analysis)

    load(transformed, analysis)
    print("✅ ETL Pipeline complete!")

if __name__ == "__main__":
    run()