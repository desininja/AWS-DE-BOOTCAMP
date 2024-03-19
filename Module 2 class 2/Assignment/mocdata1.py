import csv
import random
from datetime import datetime, timedelta

# Sample data for generating mock transactions
customer_info = {
    "John Doe": {
        "customer_id": 1001,
        "debit_card_number": '1234567890123456',
        "debit_card_type": "Visa",
        "bank_name": "Chase Bank"
    },
    "Jane Smith": {
        "customer_id": 1002,
        "debit_card_number": '2345678901234567',
        "debit_card_type": "Mastercard",
        "bank_name": "Bank of America"
    },
    "Michael Johnson": {
        "customer_id": 1003,
        "debit_card_number": '3456789012345678',
        "debit_card_type": "American Express",
        "bank_name": "Wells Fargo"
    },
    "Emily Brown": {
        "customer_id": 1004,
        "debit_card_number": '4567890123456789',
        "debit_card_type": "Discover",
        "bank_name": "Citibank"
    },
    "David Davis": {
        "customer_id": 1005,
        "debit_card_number": '5678901234567890',
        "debit_card_type": "Visa",
        "bank_name": "US Bank"
    }
}

debit_card_types = ["Visa", "Mastercard", "American Express", "Discover"]
bank_names = ["Bank of America", "Chase Bank", "Wells Fargo", "Citibank", "US Bank"]

# Function to generate mock data for daily transactions
def generate_mock_transactions(num_transactions):
    transactions = []
    for name, info in customer_info.items():
        for _ in range(num_transactions):
            transaction_date = datetime.now().strftime('%Y-%m-%d')
            amount_spend = round(random.uniform(10, 500), 2)  # Varies for each transaction
            transactions.append([info["customer_id"], name, info["debit_card_number"], info["debit_card_type"], info["bank_name"], transaction_date, amount_spend])
    
    return transactions

# Function to save transactions data to a CSV file
def save_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['customer_id', 'name', 'debit_card_number', 'debit_card_type', 'bank_name', 'transaction_date', 'amount_spend'])
        writer.writerows(data)

# Generate transactions for today
num_transactions = 10  # You can change this number as per your requirement
transactions = generate_mock_transactions(num_transactions)

# Save transactions to a CSV file
today_date = datetime.now().strftime('%Y-%m-%d')
csv_filename = f"transactions_{today_date}.csv"
save_to_csv(transactions, csv_filename)

print(f"Mock transactions for {today_date} have been generated and saved to '{csv_filename}'.")
