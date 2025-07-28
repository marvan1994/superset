import duckdb
import subprocess
import pandas as pd
from faker import Faker
import random
import numpy as np
from datetime import datetime, timedelta
import os
import uuid
import json

# --- Configuration ---
# Set a seed for reproducibility
random.seed(42)
np.random.seed(42)
Faker.seed(42)

# File path for the database inside the Docker container
DB_FILE = '/data/analytics.db'

# Number of records for each table
NUM_CAMPAIGNS = 30
NUM_AD_CLICKS = 10000
NUM_TEMP_USERS = 5000  # Anonymous users who click ads
NUM_REGISTERED_USERS = int(NUM_TEMP_USERS * 0.8) # 80% of clickers sign up
NUM_USER_ACTIVITIES = 100000
NUM_USER_ORDERS = 10000
NUM_STOCKS = 100
NUM_ORDER_ITEMS_MIN = 1
NUM_ORDER_ITEMS_MAX = 5 # Each order can have 1 to 5 stocks

# --- Initialization ---
fake = Faker()

# --- Reusable Data Pools ---
PLATFORMS = ['Google', 'Facebook', 'Instagram', 'LinkedIn', 'TikTok']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
SIGNUP_PLATFORMS = ['ios', 'android', 'web']
ACTIVITY_TYPES = ['view_stock', 'search', 'deposit', 'withdraw', 'watchlist_add']
ORDER_STATUSES = ['completed', 'pending', 'cancelled']
ORDER_STATUS_WEIGHTS = [0.85, 0.1, 0.05] # Most orders are completed
SECTORS = ['Technology', 'Finance', 'Healthcare', 'Energy', 'Retail', 'Industrials', 'Consumer Staples']
COUNTRIES = [fake.country() for _ in range(30)]
REFERRAL_CODES = [f'REF{i:03d}' for i in range(50)]

# --- Generation Functions ---

def generate_stocks():
    print("1. Generating 'stocks' table...")
    stocks = []
    tickers = set()
    while len(stocks) < NUM_STOCKS:
        company = fake.company()
        name_parts = company.replace(',', '').split(' ')
        if len(name_parts) > 1 and len(name_parts[0]) > 2:
            ticker = name_parts[0][:4].upper()
            if ticker not in tickers:
                tickers.add(ticker)
                stocks.append({
                    'stock_id': ticker,
                    'stock_name': f"{company}",
                    'ticker': ticker,
                    'sector': random.choice(SECTORS)
                })
    return pd.DataFrame(stocks)

def generate_ad_campaigns():
    print("2. Generating 'ad_campaigns' table...")
    data = []
    for _ in range(NUM_CAMPAIGNS):
        start_date = fake.date_time_between(start_date='-90d', end_date='-30d')
        end_date = start_date + timedelta(days=random.randint(15, 60))
        spend = round(random.uniform(100.0, 10000.0), 2)
        impressions = random.randint(1000, 1000000)
        clicks = random.randint(50, min(50000, int(impressions * 0.1))) # Clicks can't exceed impressions
        quarter = (start_date.month - 1) // 3 + 1

        data.append({
            'campaign_id': str(uuid.uuid4()),
            'platform': random.choice(PLATFORMS),
            'campaign_name': f"{random.choice(SECTORS)} Push Q{quarter} {start_date.year}",
            'spend_usd': spend,
            'impressions': impressions,
            'clicks': clicks,
            'start_date': start_date,
            'end_date': end_date
        })
    return pd.DataFrame(data)

def generate_ad_clicks(campaigns_df):
    print("3. Generating 'ad_clicks' table...")
    data = []
    temp_user_ids = [str(uuid.uuid4()) for _ in range(NUM_TEMP_USERS)]
    
    # Create a lookup for campaign dates to speed up generation
    campaign_dates = campaigns_df.set_index('campaign_id')[['start_date', 'end_date']].to_dict('index')

    for _ in range(NUM_AD_CLICKS):
        campaign_id = random.choice(campaigns_df['campaign_id'])
        campaign_info = campaign_dates[campaign_id]
        data.append({
            'click_id': str(uuid.uuid4()),
            'user_temp_id': random.choice(temp_user_ids),
            'campaign_id': campaign_id,
            'click_timestamp': fake.date_time_between(start_date=campaign_info['start_date'], end_date=campaign_info['end_date']),
            'device_type': random.choice(DEVICE_TYPES),
            'country': random.choice(COUNTRIES)
        })
    return pd.DataFrame(data)

def generate_users(ad_clicks_df):
    print("4. Generating 'users' table...")
    # Find the first click for each temp user for attribution
    first_clicks = ad_clicks_df.sort_values('click_timestamp').drop_duplicates('user_temp_id', keep='first')
    
    # We'll only register a subset of users who clicked
    potential_users = first_clicks.sample(n=NUM_REGISTERED_USERS)

    data = []
    for _, row in potential_users.iterrows():
        data.append({
            'user_id': str(uuid.uuid4()),
            'user_temp_id': row['user_temp_id'],
            'registered_at': row['click_timestamp'] + timedelta(minutes=random.randint(1, 120)),
            'country': row['country'], # Inherit country from the first click
            'signup_platform': random.choice(SIGNUP_PLATFORMS),
            'referred_by': random.choice([None] * 5 + REFERRAL_CODES) # Most users are not referred
        })
    return pd.DataFrame(data)

def generate_user_activity(users_df, stocks_df):
    print("5. Generating 'user_activity' table...")
    data = []
    user_pool = users_df[['user_id', 'registered_at']].to_dict('records')
    stock_ids = list(stocks_df['stock_id'])

    for _ in range(NUM_USER_ACTIVITIES):
        user = random.choice(user_pool)
        activity_type = random.choice(ACTIVITY_TYPES)
        metadata = {}
        
        if activity_type in ['view_stock', 'watchlist_add']:
            metadata['stock_id'] = random.choice(stock_ids)
        elif activity_type == 'search':
            metadata['search_term'] = fake.word()
        elif activity_type == 'deposit':
             metadata['amount_usd'] = round(random.uniform(50.0, 2000.0), 2)

        data.append({
            'activity_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'activity_type': activity_type,
            'activity_timestamp': fake.date_time_between(start_date=user['registered_at'], end_date='now'),
            'metadata': json.dumps(metadata) if metadata else None
        })
    return pd.DataFrame(data)

def generate_user_orders(users_df):
    print("6. Generating 'user_orders' table (preliminary)...")
    data = []
    
    # Power-law distribution: a few users make many orders
    # The 'a' parameter controls the skew. > 1. Higher 'a' means more inequality.
    zipf_dist = np.random.zipf(a=1.5, size=NUM_USER_ORDERS)
    # Clamp values to be valid user indices
    user_indices = zipf_dist % len(users_df) - 1

    users_by_index = users_df.set_index(pd.Index(range(len(users_df))))
    
    for i in range(NUM_USER_ORDERS):
        user_index = user_indices[i]
        user = users_by_index.iloc[user_index]
        
        data.append({
            'order_id': i + 1,  # Sequential integer IDs
            'user_id': user['user_id'],
            'order_timestamp': fake.date_time_between(start_date=user['registered_at'], end_date='now'),
            'total_amount_usd': 0.0, # Will be calculated later
            'status': random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]
        })
    return pd.DataFrame(data)

def generate_order_items(orders_df, stocks_df):
    print("7. Generating 'order_items' table...")
    data = []
    stock_ids = list(stocks_df['stock_id'])

    for _, order in orders_df.iterrows():
        # Only create items for completed or pending orders
        if order['status'] == 'cancelled':
            continue
            
        num_items = random.randint(NUM_ORDER_ITEMS_MIN, NUM_ORDER_ITEMS_MAX)
        chosen_stocks = random.sample(stock_ids, k=num_items)
        
        for stock_id in chosen_stocks:
            data.append({
                'order_item_id': str(uuid.uuid4()),
                'order_id': order['order_id'],
                'stock_id': stock_id,
                'quantity': random.randint(1, 100),
                'price_per_stock': round(random.uniform(10.0, 3000.0), 2)
            })
    return pd.DataFrame(data)
def generate_ab_test_data(n_users=10000, test_days=14):
    """
    Generates a DataFrame of synthetic A/B test results for a UI block placement test.
    Returns two DataFrames: one with raw user-level results, one with a daily summary.
    """
    print(f"Generating A/B test data for {n_users} users over {test_days} days...")
    
    users = []
    start_date = datetime.now() - timedelta(days=test_days)

    for user_id in range(n_users):
        variant = np.random.choice(['A', 'B'])
        was_exposed, clicked, converted = False, False, False
        
        if variant == 'A':  # Top of screen: High exposure, lower intent
            if np.random.rand() < 0.95:
                was_exposed = True
                if np.random.rand() < 0.10:
                    clicked = True
                    if np.random.rand() < 0.40:
                        converted = True
        else:  # Variant B - Bottom of screen: Low exposure, higher intent
            if np.random.rand() < 0.30:
                was_exposed = True
                if np.random.rand() < 0.25:
                    clicked = True
                    if np.random.rand() < 0.60:
                        converted = True

        users.append({
            'user_id': user_id, 'variant': variant, 'was_exposed': was_exposed,
            'clicked': clicked, 'converted': converted,
            'timestamp': fake.date_time_between(start_date=start_date, end_date='now'),
            'device_type': np.random.choice(['iOS', 'Android'], p=[0.6, 0.4]),
            'country': fake.country()
        })

    ab_test_results = pd.DataFrame(users)
    
    # Create the daily summary table
    ab_test_results['test_date'] = ab_test_results['timestamp'].dt.date
    ab_test_daily_summary = ab_test_results.groupby(['test_date', 'variant']).agg(
        users=('user_id', 'nunique'),
        exposed_users=('was_exposed', lambda x: x.sum()),
        clicks=('clicked', lambda x: x.sum()),
        conversions=('converted', lambda x: x.sum())
    ).reset_index()
    
    print("A/B test data generation complete.")
    return ab_test_results, ab_test_daily_summary
def main():
    """Main function to generate all data and write to DuckDB."""
    # Ensure the /data directory exists
    db_dir = os.path.dirname(DB_FILE)
    if not os.path.exists(db_dir):
        print(f"Creating directory: {db_dir}")
        os.makedirs(db_dir)
    
    # --- Generate DataFrames in order of dependency ---
    df_stocks = generate_stocks()
    df_campaigns = generate_ad_campaigns()
    df_ad_clicks = generate_ad_clicks(df_campaigns)
    df_users = generate_users(df_ad_clicks)
    df_activities = generate_user_activity(df_users, df_stocks)
    ab_results_df, ab_summary_df = generate_ab_test_data(n_users=10000)
    # Special handling for orders and items to ensure total_amount is correct
    df_orders_prelim = generate_user_orders(df_users)
    df_order_items = generate_order_items(df_orders_prelim, df_stocks)

    print("8. Recalculating 'user_orders.total_amount_usd' for consistency...")
    order_totals = df_order_items.groupby('order_id').apply(
        lambda x: (x['quantity'] * x['price_per_stock']).sum()
    ).reset_index(name='calculated_total')
    
    # Merge calculated totals back into the orders table
    df_orders = pd.merge(df_orders_prelim, order_totals, on='order_id', how='left')
    df_orders['total_amount_usd'] = df_orders['calculated_total'].fillna(0).round(2)
    df_orders.drop(columns=['calculated_total'], inplace=True)
    
    # --- Connect to DB and Write Tables ---
    print(f"\nConnecting to DuckDB file at {DB_FILE}...")
    con = duckdb.connect(database=DB_FILE, read_only=False)

    tables = {
        'stocks': df_stocks,
        'ad_campaigns': df_campaigns,
        'ad_clicks': df_ad_clicks,
        'users': df_users,
        'user_activity': df_activities,
        'user_orders': df_orders,
        'order_items': df_order_items
    }

    for name, df in tables.items():
        print(f"  - Writing table '{name}'...")
        # Using CREATE OR REPLACE TABLE to make the script idempotent
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df")
    
    print("\n--- Verification ---")
    for table_name in tables.keys():
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Table '{table_name}' contains {count} rows.")
    
        # --- Write ab_test_results table ---
    table_name_ab_results = 'ab_test_results'
    print(f"Writing table '{table_name_ab_results}'...")
    con.execute(f"CREATE OR REPLACE TABLE {table_name_ab_results} AS SELECT * FROM ab_results_df")
    count = con.execute(f"SELECT COUNT(*) FROM {table_name_ab_results}").fetchone()[0]
    print(f"Successfully inserted {count} rows into '{table_name_ab_results}'.")
    
    # --- Write ab_test_daily_summary table ---
    table_name_ab_summary = 'ab_test_daily_summary'
    print(f"Writing table '{table_name_ab_summary}'...")
    con.execute(f"CREATE OR REPLACE TABLE {table_name_ab_summary} AS SELECT * FROM ab_summary_df")
    count = con.execute(f"SELECT COUNT(*) FROM {table_name_ab_summary}").fetchone()[0]
    print(f"Successfully inserted {count} rows into '{table_name_ab_summary}'.")
    
    con.close()
    print("\nDatabase seeding complete. DuckDB connection closed.")
    
        
    try:
        subprocess.run(["chown", "1000:1000", DB_FILE], check=True)
        print(f"Changed ownership of {DB_FILE} to UID 1000")
    except Exception as e:
        print(f"Failed to change ownership of DB file: {e}")

if __name__ == "__main__":
    main()