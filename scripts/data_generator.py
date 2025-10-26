import random
from datetime import datetime, timedelta
from scripts.db_connection import get_db_connection

# ---------------------------
# 🔧 MySQL Database Connection
# ---------------------------
connection = get_db_connection()


cursor = connection.cursor()

# ---------------------------
# 🧱 Create Table if Not Exists
# ---------------------------
create_table_query = """
CREATE TABLE IF NOT EXISTS transactions (
  txn_id INT AUTO_INCREMENT PRIMARY KEY,
  account_id INT,
  beneficiary_id INT,
  amount DECIMAL(10,2),
  tx_type VARCHAR(20),
  channel VARCHAR(20),
  timestamp DATETIME,
  device_id VARCHAR(100),
  location VARCHAR(100),
  is_fraud BOOLEAN DEFAULT 0
);
"""
cursor.execute(create_table_query)

# ---------------------------
# ⚙️ Configuration
# ---------------------------
num_records = 10000
tx_types = ["NEFT", "RTGS", "UPI", "IMPS", "QuickPay"]
channels = ["MobileApp", "Web", "ATM", "Branch"]
locations = ["Mumbai", "Delhi", "Pune", "Chennai", "Bangalore", "Hyderabad", "Kolkata", "Ahmedabad"]
devices = [f"DEV{1000+i}" for i in range(1, 101)]

def random_timestamp():
    start_date = datetime.now() - timedelta(days=60)
    random_days = random.randint(0, 59)
    random_seconds = random.randint(0, 86400)
    return (start_date + timedelta(days=random_days, seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------
# 🚀 Insert Random Data
# ---------------------------
insert_query = """
INSERT INTO transactions 
(account_id, beneficiary_id, amount, tx_type, channel, timestamp, device_id, location, is_fraud)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for _ in range(num_records):
    account_id = random.randint(1001, 1100)
    beneficiary_id = random.randint(2001, 2300)
    amount = round(random.uniform(100, 200000), 2)
    tx_type = random.choice(tx_types)
    channel = random.choice(channels)
    timestamp = random_timestamp()
    device_id = random.choice(devices)
    location = random.choice(locations)
    is_fraud = 1 if random.random() < 0.02 else 0  # 2% fraud rate

    cursor.execute(insert_query, (
        account_id, beneficiary_id, amount, tx_type, channel,
        timestamp, device_id, location, is_fraud
    ))

connection.commit()
print(f"✅ Inserted {num_records} random transaction records successfully!")

# ---------------------------
# 🔒 Close Connection
# ---------------------------
cursor.close()
connection.close()
