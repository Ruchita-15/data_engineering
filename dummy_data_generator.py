import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

NUM_CUSTOMERS = 1000
NUM_ACCOUNTS = 1500
NUM_TRANSACTIONS = 20000

CUSTOMER_SEGMENTS = ["Retail", "Priority", "Corporate"]
ACCOUNT_TYPES = ["Savings", "Current", "Credit"]
TRANSACTION_TYPES = ["CARD", "TRANSFER", "ONLINE", "ATM"]
STATUSES = ["SUCCESS", "FAILED"]
COUNTRIES = ["UAE", "IN", "US", "UK", "SG"]
MERCHANTS = ["Amazon", "Noon", "Careem", "Talabat", "Apple", "Netflix"]

# -----------------------------
# Generate Customers
# -----------------------------
customers = []
for i in range(NUM_CUSTOMERS):
    customers.append({
        "customer_id": f"CUST{i+1:05d}",
        "customer_name": fake.name(),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        "country": random.choice(COUNTRIES),
        "segment": random.choice(CUSTOMER_SEGMENTS)
    })

with open("customers.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=customers[0].keys()
    )
    writer.writeheader()
    writer.writerows(customers)

# -----------------------------
# Generate Accounts
# -----------------------------
accounts = []
for i in range(NUM_ACCOUNTS):
    cust = random.choice(customers)
    accounts.append({
        "account_id": f"ACC{i+1:06d}",
        "customer_id": cust["customer_id"],
        "account_type": random.choice(ACCOUNT_TYPES),
        "balance": round(random.uniform(500, 200000), 2),
        "created_date": fake.date_between(start_date="-10y", end_date="today").isoformat()
    })

with open("accounts.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=accounts[0].keys()
    )
    writer.writeheader()
    writer.writerows(accounts)

# -----------------------------
# Generate Transactions
# -----------------------------
transactions = []
for i in range(NUM_TRANSACTIONS):
    acc = random.choice(accounts)
    txn_time = datetime.now() - timedelta(days=random.randint(0, 365))

    transactions.append({
        "transaction_id": f"TXN{i+1:08d}",
        "account_id": acc["account_id"],
        "customer_id": acc["customer_id"],
        "amount": round(random.uniform(10, 50000), 2),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "merchant": random.choice(MERCHANTS),
        "country": random.choice(COUNTRIES),
        "transaction_timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": random.choice(STATUSES)
    })

with open("transactions.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=transactions[0].keys()
    )
    writer.writeheader()
    writer.writerows(transactions)

print("✅ Dummy banking data generated successfully!")
