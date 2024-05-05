import json
import random

import csv
import random
from faker import Factory
from faker.providers import credit_card
import boto3





faker = Factory.create()

complete_transaction_record = 'transaction_id' + ',' + 'customer_id' + ',' + 'first_name' + ',' + 'last_name' + ',' + 'email' + ',' + 'membership_level' + ',' + 'product_id' + ',' + 'product_name' + ',' + 'category' + ',' + 'quantity' + ',' + 'price' + ',' + 'supplier_id' + 'transaction_date' + ',' + 'payment_type' + ',' + 'status' + '\n'
transaction_record = 'transaction_id' + ',' + 'customer_id' + ',' + 'product_id' + ','  + 'quantity' + ',' + 'price' + ',' + 'transaction_date' + ',' + 'payment_type' + ',' + 'status' + '\n'
product_record = 'product_id' + ',' + 'product_name' + ',' + 'category' + ',' + 'price' + ',' + 'supplier_id' + '\n'
customer_record = 'customer_id' + ',' + 'first_name' + ',' + 'last_name' + ',' + 'email' + ',' + 'membership_level' + '\n'
transaction_csv = 'transaction_id' + ',' + 'transaction_date' + ',' + 'product_id' + ',' + 'quantity' + ',' + 'price' + ',' + 'store_location' + '\n'

transaction_csv_lst = []
transaction_csv_lst.append(transaction_csv)


# Function to generate synthetic ecommerce data
def generate_synthetic_ecommerce_data(num_records):
    products = {
        "Electronics": ["Smartphone", "Laptop", "Tablet", "TV", "Camera", "Headphones", "Smartwatch", "Gaming Console",
                        "Speaker"],
        "Clothing & Apparel": ["T-Shirt", "Jeans", "Dress", "Jacket", "Shoes", "Hoodie", "Sweater", "Skirt", "Shorts"],
        "Home & Garden": ["Furniture", "Bedding", "Kitchenware", "Home Decor", "Appliances", "Lighting",
                          "Bathroom Accessories", "Gardening Tools"],
        "Beauty & Personal Care": ["Skincare", "Makeup", "Haircare", "Perfume", "Nail Care", "Personal Care Appliances",
                                   "Men's Grooming", "Beauty Tools"],
        "Sports & Outdoors": ["Athletic Shoes", "Outdoor Clothing", "Fitness Equipment", "Camping Gear", "Cycling Gear",
                              "Hiking Gear", "Water Sports Gear", "Team Sports Equipment"],
        "Books & Media": ["Fiction Books", "Non-Fiction Books", "Children's Books", "Textbooks", "Magazines", "DVDs",
                          "Blu-rays", "Video Games"]
    }

    payment_type = ["Credit/Debit Cards", "Digital Wallets", "Bank Transfers", "Cash on Delivery (COD)",
                    "Buy Now, Pay Later (BNPL)", "Cryptocurrency", "Gift Cards", "Mobile Payments", "Prepaid Cards"]

    order_statuses = ["Pending", "Processing", "Confirmed", "Shipped", "Out for Delivery", "Delivered", "Cancelled",
                      "Refunded", "On Hold", "Returned", "Completed"]
    email_domain = ['gmail.com', 'outlook.com', 'yahoo.com', 'microsoft.com', 'rediffmail.com', 'example.com',
                    'neo.com', 'aol.com', 'icloud.com', 'mainframe.com', 'alibaba.com']

    membership_level = ['Gold', 'Silver', 'Bronze', 'Platinum', 'Premium', 'Gold-Premium']

    # membership_level = ['Gold', 'Silver', 'Bronze', 'Platinum', 'Premium', 'Gold-Premium']

    synthetic_data = []
    transaction_data =[]
    transaction_data_json =[]
    product_data = []
    customer_data = []

    synthetic_data.append(complete_transaction_record)
    transaction_data.append(transaction_record)
    product_data.append(product_record)
    customer_data.append(customer_record)


    for _ in range(num_records):

        if _ == 0 :
            record_csv = 'transaction_id' + ',' + 'customer_id' ',' + 'first_name' + ',' + 'last_name' + ',' + 'email' + ',' + 'membership_level' + 'product_id' + 'product_name' + 'category' + ',' + 'quantity' + ',' + 'price' + ',' + 'supplier_id' + 'transaction_date' + ',' + 'payment_type' + 'status'
        else:
            random_id = random.randint(1,10000)
            transaction_id = 'TXN' + str(random.randint(1, 10000000000000000000)).rjust(20,'0')
            customer_id = 'C' + str(random.randint(1, 1000000000)).rjust(15, '0')
            customer_full_name = faker.name()
            customer_first_name = customer_full_name.split(' ')[0]
            customer_last_name = customer_full_name.split(' ')[1]
            email = customer_first_name + '.' + customer_last_name + '@' + random.choice(email_domain)
            membership_lvl = random.choice(membership_level)
            product_id = 'P' + str(random.randint(1, 1000000)).rjust(10, '0')
            supplier_id = 'S' + str(random.randint(1, 1000000)).rjust(10, '0')
            transaction_date = str(faker.date())
            payment_typ = random.choice(payment_type)
            order_status = random.choice(order_statuses)
            store_location = faker.state()
            category = random.choice(list(products.keys()))
            product = random.choice(products[category])
            price = str(round(random.uniform(10, 1000), 2))
            quantity = str(random.randint(1, 100))
            # brand = ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
            # rating = round(random.uniform(1, 5), 1)

            print("1",transaction_id)
            print("2",transaction_date)
            print("3",product_id)
            print("4",quantity)
            print("5",price)
            print("6",store_location)


            transaction_csv = (
             transaction_id + ',' +
             transaction_date + ',' +
             product_id + ',' +
             quantity + ',' +
             price + ',' +
             store_location + '\n'
                            )

            transaction_csv_lst.append(transaction_csv)

    return transaction_csv_lst

#
# randomly generating records per file
numOfRecords = random.randint(1,100)
# Exam?ple usage

num_records = numOfRecords
# complete_transactionFile = "C:\\Users\\Admin\\Documents\\study\\AWS Grow Data Skills\\Week3\\assignments\\Complete_Transactions_" + str(
#     num_records) + ".csv"
transactionFile = "C:\\Users\\Admin\\Documents\\study\\AWS Grow Data Skills\\Week5\\Day2\\assignments\\Transactions_" + str(
    num_records) + ".csv"
# transactionFileJson = "C:\\Users\\Admin\\Documents\\study\\AWS Grow Data Skills\\Week3\\assignments\\Transactions_" + str(num_records) + '_' + str(
#     str(faker.date())) + ".json"
#
# productsDimFile = "C:\\Users\\Admin\\Documents\\study\\AWS Grow Data Skills\\Week3\\assignments\\products_dimension_" + str(
#     num_records) + ".csv"
# customersDimFile = "C:\\Users\\Admin\\Documents\\study\\AWS Grow Data Skills\\Week3\\assignments\\customers_dimension_" + str(
#     num_records) + ".csv"

# synthetic_ecommerce_data , tran_data_json, tran_data , product_dim_data , customer_dim_data  = generate_synthetic_ecommerce_data(num_records)

transaction_csv_lst  = generate_synthetic_ecommerce_data(num_records)

print('-------numOfRecords',numOfRecords)
# for i in transaction_csv_lst:
#     print(i)

with open(transactionFile, 'a') as fileHandle:
    for _ in transaction_csv_lst:
        fileHandle.write(_)


s3_client = boto3.client('s3')
bucket_name = 'incrementaldataprocessing'
object_key = "Transactions_" + str(num_records) + ".csv"
s3_client.upload_file(transactionFile, bucket_name, object_key)

# print("length of json ",len(tran_data_json))
# with open(transactionFileJson, 'a') as fileHandle:
#     fileHandle.write(json.dumps(tran_data_json))

# with open(transactionFileJson, 'a') as fileHandle:
#     fileHandle.write(json.dumps(tran_data_json))

# for i in range(0,len(synthetic_ecommerce_data)):
    # with open(complete_transactionFile, 'a') as fileHandle:
    #     fileHandle.write(synthetic_ecommerce_data[i])
    # below manipulation for json is rquired to create a properly parsable file by adding '[' at the beginning and ']' at the end
    # also wanted to add',' at the end of each record and to put each record on a new line
    # this can be done with json.dumps but it puts all the records in one line
    # if ( i <= len(synthetic_ecommerce_data) - 2 ):
    #     with open(transactionFileJson, 'a') as jsonFileHandle:
    #         if ( i == 0 ):
    #             jsonFileHandle.write('[ \n')
    #         json.dump(tran_data_json[i] , jsonFileHandle )
    #         if ( i == len(synthetic_ecommerce_data) - 2):
    #             jsonFileHandle.write('\n]')
    #         else:
    #             jsonFileHandle.write(',\n')
    # with open(transactionFile, 'a') as fileHandle:
    #     fileHandle.write(tran_data[i])
    # with open(productsDimFile, 'a') as fileHandle:
    #     fileHandle.write(product_dim_data[i])
    # with open(customersDimFile, 'a') as fileHandle:
    #     fileHandle.write(customer_dim_data[i])

