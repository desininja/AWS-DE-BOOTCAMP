import csv
import random
from datetime import datetime, timedelta

# Function to generate mock data for daily transactions
def generate_mock_transactions(num_transactions):
    # Sample data for generating mock transactions
    customer_names = ["Himanshu Bhatt", "Vaibhavi Nautiyal", "Naruto Uzumaki", "Sasuke Uchiha", "Tanjiro Kamado", "Itadori Sukuna"]
    debit_card_types = ["Visa", "Mastercard", "American Express", "Discover"]
    bank_names = ["Bank of America", "HDFC", "SBI", "Citibank", "Standard Chartered"]
    
    # Generate transactions
    transactions = []
    for _ in range(num_transactions):
        customer_id = random.randint(1000, 9999)
        name = random.choice(customer_names)
        debit_card_number = ''.join(str(random.randint(0, 9)) for _ in range(16))
        debit_card_type = random.choice(debit_card_types)
        bank_name = random.choice(bank_names)
        transaction_date = datetime.now().strftime('%Y-%m-%d')
        amount_spend = round(random.uniform(10, 500), 2)
        
        transactions.append([customer_id, name, debit_card_number, debit_card_type, bank_name, transaction_date, amount_spend])
    
    return transactions

# Function to save transactions data to a CSV file
def save_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['customer_id', 'name', 'debit_card_number', 'debit_card_type', 'bank_name', 'transaction_date', 'amount_spend'])
        writer.writerows(data)

# Generate transactions for today
num_transactions = 50  # You can change this number as per your requirement
transactions = generate_mock_transactions(num_transactions)

# Save transactions to a CSV file
today_date = datetime.now().strftime('%Y-%m-%d')
csv_filename = f"transactions_{today_date}.csv"
save_to_csv(transactions, csv_filename)

print(f"Mock transactions for {today_date} have been generated and saved to '{csv_filename}'.")
