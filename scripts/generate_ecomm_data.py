import csv
import random
import uuid
import sys
import argparse
from datetime import datetime, timedelta
from faker import Faker

# --- Configuration ---
NUM_CUSTOMERS = 100 # Reduced for quicker testing/visualization
NUM_PRODUCTS = 500
NUM_ORDERS = 5000
BACKFILL_PERIOD_DAYS = 365
INCREMENTAL_PERIOD_DAYS = 7 
INCREMENTAL_STEP_DAYS = 1 # Define the size of each incremental step (1 day)
NUM_INCREMENTAL_STEPS = 2 # Run incremental generation 2 times
CUSTOMER_UPDATE_RATE = 0.005 # 0.5% of existing customers change

# Initialize Faker
fake = Faker()

# Real FMCG Products with Categories (Keeping this section concise for the answer)
FMCG_PRODUCTS = {
    "Coca-Cola Classic": "Beverages", "Pepsi": "Beverages", "Red Bull Energy Drink": "Beverages",
    "Lipton Iced Tea": "Beverages", "Nescafe Classic Coffee": "Beverages", "Starbucks Frappuccino": "Beverages",
    "Lay's Classic Potato Chips": "Packaged Foods & Snacks", "Doritos Nacho Cheese": "Packaged Foods & Snacks",
    "Kit Kat Chocolate Bar": "Packaged Foods & Snacks", "Oreo Cookies": "Packaged Foods & Snacks",
    "Dove Beauty Bar": "Personal Care & Beauty", "Crest 3D White Toothpaste": "Personal Care & Beauty",
    "Tide Laundry Detergent Pods": "Household & Cleaning", "Clorox Bleach": "Household & Cleaning",
    "Advil Pain Reliever Tablets": "Health & Wellness", "Tylenol Extra Strength Caplets": "Health & Wellness",
}
# NOTE: The full FMCG_PRODUCTS dictionary from the original script should be here.

# --- Helper Functions (No changes needed) ---
def generate_timestamp(start_date, end_date):
    """Generates a random timestamp between two dates."""
    time_delta = end_date - start_date
    random_seconds = random.uniform(0, time_delta.total_seconds())
    return start_date + timedelta(seconds=random_seconds)

def write_to_csv(data, filename, fieldnames):
    """Writes a list of dictionaries to a CSV file."""
    # NOTE: You must create the 'datasets/syn_ecomm_dataset/' directory before running this script
    filename = f"datasets/syn_ecomm_dataset/{filename}"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# --- Data Generation Functions (No changes needed) ---

def generate_customers(num_customers, start_date, end_date):
    """Generates customer data using Faker (for initial creation)."""
    customers = []
    for _ in range(num_customers):
        created_at = generate_timestamp(start_date, end_date)
        updated_at = created_at
        
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(10, 99)}@example.com"
        
        customer = {
            'customer_id': str(uuid.uuid4()),
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        customers.append(customer)
    return customers

def generate_customer_updates(existing_customers, update_rate, incremental_start_date, incremental_end_date):
    """Generates updates for a subset of existing customers."""
    updated_customers = []
    num_to_update = int(len(existing_customers) * update_rate)
    
    if not existing_customers:
        return []

    customers_to_update = random.sample(existing_customers, min(num_to_update, len(existing_customers)))
    
    for customer in customers_to_update:
        updated_customer = customer.copy()
        
        change_type = random.choice(['last_name', 'email'])
        updated_at = generate_timestamp(incremental_start_date, incremental_end_date)
        
        if change_type == 'last_name':
            new_last_name = fake.last_name()
            while new_last_name == updated_customer['last_name']:
                new_last_name = fake.last_name()
            
            updated_customer['last_name'] = new_last_name
            updated_customer['email'] = f"{updated_customer['first_name'].lower()}.{new_last_name.lower()}{random.randint(10, 99)}@example.com"
        
        elif change_type == 'email':
            new_email = f"{updated_customer['first_name'].lower()}.{updated_customer['last_name'].lower()}{random.randint(100, 999)}@example.com"
            updated_customer['email'] = new_email
            
        updated_customer['updated_at'] = updated_at.isoformat()
        updated_customers.append(updated_customer)
        
    return updated_customers

def generate_products(num_products, start_date, end_date):
    """Generates product data with real names and correctly linked categories."""
    products = []
    product_names = list(FMCG_PRODUCTS.keys())
    product_names_subset = random.sample(product_names, min(num_products, len(product_names)))
    
    for product_name in product_names_subset:
        created_at = generate_timestamp(start_date, end_date)
        updated_at = created_at
        
        product = {
            'product_id': str(uuid.uuid4()),
            'product_name': product_name,
            'price': round(random.uniform(2.0, 50.0), 2),
            'category': FMCG_PRODUCTS[product_name],
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        products.append(product)
    return products

def generate_orders(num_orders, customers, products, start_date, end_date):
    """Generates order data, with corrected total_amount calculation."""
    orders = []
    order_items = []
    
    customer_ids = [c['customer_id'] for c in customers]
    
    for _ in range(num_orders):
        created_at = generate_timestamp(start_date, end_date)
        updated_at = created_at
        
        if not customer_ids: continue

        customer_id = random.choice(customer_ids)
        order_id = str(uuid.uuid4())
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': created_at.isoformat(),
            'status': random.choice(['pending', 'shipped', 'delivered', 'cancelled']),
            'total_amount': 0.0,
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        
        num_items = random.randint(1, 4)
        total_amount = 0.0
        
        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 3)

            item_subtotal = product['price'] * quantity
            item_price_rounded = round(item_subtotal, 2)
            total_amount += item_price_rounded

            order_item = {
                'order_item_id': str(uuid.uuid4()),
                'order_id': order_id,
                'product_id': product['product_id'],
                'unit_price': product['price'],
                'quantity': quantity,
                'item_price': item_price_rounded,
                'created_at': created_at.isoformat(),
                'updated_at': updated_at.isoformat()
            }
            order_items.append(order_item)
           
        order['total_amount'] = round(total_amount, 2)
        orders.append(order)

    return orders, order_items

# --- Execution Functions ---

def run_backfill(backfill_start_date, backfill_end_date):
    """Generates and writes backfill data."""
    print("Generating backfill data...")
    
    customers_backfill = generate_customers(NUM_CUSTOMERS, backfill_start_date, backfill_end_date)
    products_backfill = generate_products(NUM_PRODUCTS, backfill_start_date, backfill_end_date)
    orders_backfill, order_items_backfill = generate_orders(NUM_ORDERS, customers_backfill, products_backfill, backfill_start_date, backfill_end_date)

    write_to_csv(customers_backfill, 'customers_backfill.csv', ['customer_id', 'first_name', 'last_name', 'email', 'created_at', 'updated_at'])
    write_to_csv(products_backfill, 'products_backfill.csv', ['product_id', 'product_name', 'price', 'category', 'created_at', 'updated_at'])
    write_to_csv(orders_backfill, 'orders_backfill.csv', ['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'created_at', 'updated_at'])
    write_to_csv(order_items_backfill, 'order_items_backfill.csv', ['order_item_id', 'order_id', 'product_id', 'unit_price', 'quantity', 'item_price', 'created_at', 'updated_at'])
    
    print("Backfill data generation complete.")
    return customers_backfill, products_backfill

def run_incremental_steps(initial_customers, initial_products, start_date, num_steps):
    """Generates and writes incremental data over multiple steps/days."""
    
    current_customers = initial_customers[:]
    current_products = initial_products[:]
    
    for i in range(1, num_steps + 1):
        # FIX: Calculate step_start_date correctly
        step_start_date = start_date + timedelta(days=(i - 1) * INCREMENTAL_STEP_DAYS)
        # FIX: Calculate step_end_date correctly (step_start_date + INCREMENTAL_STEP_DAYS)
        step_end_date = start_date + timedelta(days=i * INCREMENTAL_STEP_DAYS)
        
        # Use YYYYMMDD format for the incremental date partition, based on the start date
        incremental_date_str = step_start_date.strftime("%Y%m%d") 
        print(f"\nGenerating incremental data for Day {i} (Partition Date: {step_start_date.date()})...")

        # --- 1. Generate Data for this Step ---
        
        # 1.1 New Customers 
        num_new_customers = int(NUM_CUSTOMERS * 0.1)
        new_customers = generate_customers(num_new_customers, step_start_date, step_end_date)
        
        # 1.2 Customer Changes (Updates on the *current* customer population)
        customer_changes = generate_customer_updates(current_customers, CUSTOMER_UPDATE_RATE, step_start_date, step_end_date)

        # 1.3 New Products
        num_new_products = int(NUM_PRODUCTS * 0.1)
        new_products = generate_products(num_new_products, step_start_date, step_end_date)
        
        # 1.4 Orders 
        orders_per_step = int(NUM_ORDERS * 0.2 / num_steps)
        orders_incremental, order_items_incremental = generate_orders(
            orders_per_step, current_customers + new_customers, current_products + new_products, 
            step_start_date, step_end_date
        )

        # --- 2. Update Global Lists for Next Step ---
        current_customers.extend(new_customers)
        current_products.extend(new_products)

        # --- 3. Write Incremental Data ---
        
        # Write COMBINED Customer Changes (for UPSERT)
        customer_filename = f'customers_{incremental_date_str}.csv' 
        all_customer_updates = new_customers + customer_changes
        write_to_csv(all_customer_updates, customer_filename, ['customer_id', 'first_name', 'last_name', 'email', 'created_at', 'updated_at'])
        print(f"- Generated {len(all_customer_updates)} **UPSERT** customer records in **{customer_filename}**.")

        # Write NEW Products
        product_filename = f'products_{incremental_date_str}.csv'
        write_to_csv(new_products, product_filename, ['product_id', 'product_name', 'price', 'category', 'created_at', 'updated_at'])
        print(f"- Generated {len(new_products)} **NEW** product records in **{product_filename}**.")

        # Write NEW Orders and Order Items
        write_to_csv(orders_incremental, f'orders_{incremental_date_str}.csv', ['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'created_at', 'updated_at'])
        write_to_csv(order_items_incremental, f'order_items_{incremental_date_str}.csv', ['order_item_id', 'order_id', 'product_id', 'unit_price', 'quantity', 'item_price', 'created_at', 'updated_at'])
        print(f"- Generated {len(orders_incremental)} **NEW** order records.")
        
    print(f"\nAll {num_steps} incremental steps completed.")

# --- Main Script Execution (No changes needed) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data (backfill and multi-step incremental).")
    parser.add_argument(
        '--mode', 
        choices=['all', 'backfill', 'incremental'], 
        default='all', 
        help="Choose the data generation mode: 'all' (default), 'backfill', or 'incremental'."
    )
    args = parser.parse_args()

    # Define time boundaries
    backfill_end_date = datetime.now() - timedelta(days=INCREMENTAL_PERIOD_DAYS)
    backfill_start_date = backfill_end_date - timedelta(days=BACKFILL_PERIOD_DAYS)
    incremental_start_date = backfill_end_date

    customers_backfill = []
    products_backfill = []

    # 1. Run Backfill
    if args.mode in ['all', 'backfill']:
        customers_backfill, products_backfill = run_backfill(backfill_start_date, backfill_end_date)
    
    # 2. Run Incremental
    if args.mode in ['all', 'incremental']:
        
        if args.mode == 'incremental':
            print("Running incremental-only mode: Generating temporary backfill data...")
            customers_backfill = generate_customers(NUM_CUSTOMERS, backfill_start_date, backfill_end_date)
            products_backfill = generate_products(NUM_PRODUCTS, backfill_start_date, backfill_end_date)
            
        if not customers_backfill or not products_backfill:
            print("Cannot run incremental. Backfill data (customer/product IDs) is missing.")
            sys.exit(1)

        run_incremental_steps(customers_backfill, products_backfill, incremental_start_date, NUM_INCREMENTAL_STEPS)

    print("\nScript execution finished.")