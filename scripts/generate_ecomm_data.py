import csv
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

# --- Configuration ---
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
NUM_ORDERS = 5000
BACKFILL_PERIOD_DAYS = 365
INCREMENTAL_PERIOD_DAYS = 7

# Initialize Faker
fake = Faker()

# Real FMCG Products with Categories
FMCG_PRODUCTS = {
    # --- Beverages ---
    "Coca-Cola Classic": "Beverages",
    "Pepsi": "Beverages",
    "Red Bull Energy Drink": "Beverages",
    "Lipton Iced Tea": "Beverages",
    "Nescafe Classic Coffee": "Beverages",
    "Starbucks Frappuccino": "Beverages",
    "Fiji Natural Artesian Water": "Beverages",
    "Gatorade Thirst Quencher": "Beverages",
    "Tropicana Pure Premium Orange Juice": "Beverages",
    "Minute Maid Lemonade": "Beverages",

    # --- Packaged Foods & Snacks ---
    "Lay's Classic Potato Chips": "Packaged Foods & Snacks",
    "Doritos Nacho Cheese": "Packaged Foods & Snacks",
    "Kit Kat Chocolate Bar": "Packaged Foods & Snacks",
    "Oreo Cookies": "Packaged Foods & Snacks",
    "Pringles Original": "Packaged Foods & Snacks",
    "Kellogg's Frosted Flakes": "Packaged Foods & Snacks",
    "Quaker Oats Old Fashioned": "Packaged Foods & Snacks",
    "Campbell's Tomato Soup": "Packaged Foods & Snacks",
    "Kraft Macaroni & Cheese": "Packaged Foods & Snacks",
    "Heinz Ketchup": "Packaged Foods & Snacks",
    "Nutella Hazelnut Spread": "Packaged Foods & Snacks",
    "Skippy Creamy Peanut Butter": "Packaged Foods & Snacks",
    "Snickers Chocolate Bar": "Packaged Foods & Snacks",
    "Cheerios Cereal": "Packaged Foods & Snacks",
    "Tostitos Scoops Tortilla Chips": "Packaged Foods & Snacks",

    # --- Personal Care & Beauty ---
    "Dove Beauty Bar": "Personal Care & Beauty",
    "Crest 3D White Toothpaste": "Personal Care & Beauty",
    "Colgate Total Toothpaste": "Personal Care & Beauty",
    "Pantene Pro-V Shampoo": "Personal Care & Beauty",
    "Head & Shoulders Dandruff Shampoo": "Personal Care & Beauty",
    "Axe Body Spray": "Personal Care & Beauty",
    "Old Spice Deodorant": "Personal Care & Beauty",
    "Gillette Mach3 Razor": "Personal Care & Beauty",
    "Johnson's Baby Shampoo": "Personal Care & Beauty",
    "L'Oréal Paris Elvive Shampoo": "Personal Care & Beauty",
    "Nivea Crème": "Personal Care & Beauty",
    "Vaseline Intensive Care Lotion": "Personal Care & Beauty",

    # --- Household & Cleaning ---
    "Tide Laundry Detergent Pods": "Household & Cleaning",
    "Clorox Bleach": "Household & Cleaning",
    "Lysol Disinfectant Spray": "Household & Cleaning",
    "Windex Glass Cleaner": "Household & Cleaning",
    "Dawn Dish Soap": "Household & Cleaning",
    "Bounty Paper Towels": "Household & Cleaning",
    "Charmin Ultra Soft Toilet Paper": "Household & Cleaning",
    "Febreze Air Freshener": "Household & Cleaning",
    "Swiffer WetJet Floor Cleaner": "Household & Cleaning",
    "Glad Tall Kitchen Trash Bags": "Household & Cleaning",
    "Mr. Clean Magic Eraser": "Household & Cleaning",

    # --- Health & Wellness (Over-the-Counter) ---
    "Advil Pain Reliever Tablets": "Health & Wellness",
    "Tylenol Extra Strength Caplets": "Health & Wellness",
    "DayQuil Cold & Flu": "Health & Wellness",
    "Zyrtec Allergy Tablets": "Health & Wellness",
    "Band-Aid Brand Adhesive Bandages": "Health & Wellness",
    "Pepto-Bismol Liquid": "Health & Wellness",
}

# --- Helper Functions ---
def generate_timestamp(start_date, end_date):
    """Generates a random timestamp between two dates."""
    time_delta = end_date - start_date
    random_days = random.uniform(0, time_delta.total_seconds())
    return start_date + timedelta(seconds=random_days)

def write_to_csv(data, filename, fieldnames):
    """Writes a list of dictionaries to a CSV file."""
    filename = f"datasets/syn_ecomm_dataset/{filename}"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# --- Data Generation Functions ---
def generate_customers(num_customers, start_date):
    """Generates customer data using Faker."""
    customers = []
    for _ in range(num_customers):
        created_at = generate_timestamp(start_date, datetime.now())
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

def generate_products(num_products, start_date):
    """Generates product data with real names and correctly linked categories."""
    products = []
    # Get a list of just the product names (keys of the dictionary)
    product_names = list(FMCG_PRODUCTS.keys())
    
    # Randomly select a subset of product names
    product_names_subset = random.sample(product_names, min(num_products, len(product_names)))
    
    for product_name in product_names_subset:
        created_at = generate_timestamp(start_date, datetime.now())
        updated_at = created_at
        
        product = {
            'product_id': str(uuid.uuid4()),
            'product_name': product_name,
            'price': round(random.uniform(2.0, 50.0), 2),
            'category': FMCG_PRODUCTS[product_name], # Use the dictionary to get the correct category
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        products.append(product)
    return products

def generate_orders(num_orders, customers, products, start_date):
    """Generates order data."""
    orders = []
    order_items = []
    for _ in range(num_orders):
        created_at = generate_timestamp(start_date, datetime.now())
        updated_at = created_at
        customer_id = random.choice(customers)['customer_id']
        order_id = str(uuid.uuid4())
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': created_at.isoformat(),
            'status': random.choice(['pending', 'shipped', 'delivered', 'cancelled']),
            'total_amount': 0,
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        
        # Add order items (at least one per order)
        num_items = random.randint(1, 4)
        total_amount = 0
        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 3)
            item_price = product['price'] * quantity
            total_amount += item_price
            
            order_item = {
                'order_item_id': str(uuid.uuid4()),
                'order_id': order_id,
                'product_id': product['product_id'],
                'quantity': quantity,
                'item_price': round(item_price, 2),
                'created_at': created_at.isoformat(),
                'updated_at': updated_at.isoformat()
            }
            order_items.append(order_item)
        
        order['total_amount'] = round(total_amount, 2)
        orders.append(order)
        
    return orders, order_items

# --- Main Script Execution ---
if __name__ == "__main__":
    # --- Backfill Data Generation ---
    print("Generating backfill data...")
    backfill_end_date = datetime.now() - timedelta(days=INCREMENTAL_PERIOD_DAYS)
    backfill_start_date = backfill_end_date - timedelta(days=BACKFILL_PERIOD_DAYS)

    customers_backfill = generate_customers(NUM_CUSTOMERS, backfill_start_date)
    products_backfill = generate_products(NUM_PRODUCTS, backfill_start_date)
    orders_backfill, order_items_backfill = generate_orders(NUM_ORDERS, customers_backfill, products_backfill, backfill_start_date)

    write_to_csv(customers_backfill, 'customers_backfill.csv', ['customer_id', 'first_name', 'last_name', 'email', 'created_at', 'updated_at'])
    write_to_csv(products_backfill, 'products_backfill.csv', ['product_id', 'product_name', 'price', 'category', 'created_at', 'updated_at'])
    write_to_csv(orders_backfill, 'orders_backfill.csv', ['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'created_at', 'updated_at'])
    write_to_csv(order_items_backfill, 'order_items_backfill.csv', ['order_item_id', 'order_id', 'product_id', 'quantity', 'item_price', 'created_at', 'updated_at'])
    
    print("Backfill data generation complete.")

    # --- Incremental Data Generation ---
    print("\nGenerating incremental data...")
    incremental_start_date = backfill_end_date
    incremental_end_date = datetime.now()

    new_customers = generate_customers(int(NUM_CUSTOMERS * 0.1), incremental_start_date)
    all_customers = customers_backfill + new_customers
    
    new_products = generate_products(int(NUM_PRODUCTS * 0.1), incremental_start_date)
    all_products = products_backfill + new_products

    orders_incremental, order_items_incremental = generate_orders(int(NUM_ORDERS * 0.2), all_customers, all_products, incremental_start_date)

    incremental_date = incremental_end_date.strftime("%Y%m%d")

    write_to_csv(new_customers, f'customers_{incremental_date}.csv', ['customer_id', 'first_name', 'last_name', 'email', 'created_at', 'updated_at'])
    write_to_csv(new_products, f'products_{incremental_date}.csv', ['product_id', 'product_name', 'price', 'category', 'created_at', 'updated_at'])
    write_to_csv(orders_incremental, f'orders_{incremental_date}.csv', ['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'created_at', 'updated_at'])
    write_to_csv(order_items_incremental, f'order_items_{incremental_date}.csv', ['order_item_id', 'order_id', 'product_id', 'quantity', 'item_price', 'created_at', 'updated_at'])
    
    print("Incremental data generation complete.")