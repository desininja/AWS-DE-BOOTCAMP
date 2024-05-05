import json
import pg8000
import csv
import boto3
from botocore.exceptions import ClientError

# get redshift secrets via secretes manager
def get_secret():

    secret_name = "redshiftSecretes"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    secret = get_secret_value_response['SecretString']
    return secret





try:
    event = '{"version": "0", "id": "1f7b635c-14d2-d671-6a5e-a3c52c1b54cf", "detail-type": "Object Created", "source": "aws.s3", "account": "120582745863", "time": "2024-04-05T04:09:42Z", "region": "us-east-1", "resources": ["arn:aws:s3:::incrementaldataprocessing"], "detail": {"version": "0", "bucket": {"name": "incrementaldataprocessing"}, "object": {"key": "Transactions_14.csv", "size": 940, "etag": "59f757a563d085f765478de82799c23b", "sequencer": "00660F7986D160712D"}, "request-id": "7CJVS3XGWFYYYY6R", "requester": "120582745863", "source-ip-address": "109.152.200.112", "reason": "PutObject"}}'
    event = json.loads(event)
    print("---------------- event------------", event)

    s3 = boto3.client('s3')

    # Specify the bucket name and CSV file key
    bucket_name = event['detail']['bucket']['name']
    file_key = event['detail']['object']['key']

    print("Bucket Name is ----->{}".format(bucket_name))
    print("file Name is ----->{}".format(file_key))


    # Get the CSV file object from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')

    # get the secrete values
    secret = json.loads(get_secret())
    # redshift connectivity details
    db_host = secret['host']
    db_port = secret['port']
    db_user = secret['username']
    db_password = secret['password']
    db_name = 'dev'

    conn = pg8000.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    # Create a cursor
    cursor = conn.cursor()


    # Read the CSV content and insert it into stage file
    csv_reader = csv.reader(file_content.splitlines())

    #  TRUNCATE STAGE TABLE
    cursor.execute("TRUNCATE TABLE incremental_data_processing.fact_transactions_stg")

    # load stage table
    for row in csv_reader:
        print(row)  # Process each row as needed
        if row[0] != 'transaction_id':
            cursor.execute("INSERT INTO incremental_data_processing.fact_transactions_stg  VALUES (%s, %s, %s, %s, %s, %s)", (row[0],row[1],row[2],row[3],row[4],row[5]))

    # Commit the transaction
    conn.commit()

    # Execute a query
    # cursor.execute("SELECT * FROM incremental_data_processing.fact_transactions_stg ")


    # upsert logic in main table based on stage table, stage table is loaded using s3 file( stage is truncate and load)
    cursor.execute("""
                        MERGE INTO INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS 
                        USING INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG 
                        ON INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS.TRANSACTION_ID  = INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.TRANSACTION_ID
                        WHEN MATCHED THEN
                        UPDATE SET 
                        TRANSACTION_DATE = INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.TRANSACTION_DATE ,
                        PRODUCT_ID = INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.PRODUCT_ID,
                        QUANTITY = INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.QUANTITY,
                        PRICE = INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.PRICE,
                        STORE_LOCATION = INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.STORE_LOCATION
                        WHEN NOT MATCHED THEN
                        INSERT VALUES (INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.TRANSACTION_ID,INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.TRANSACTION_DATE,INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.PRODUCT_ID,INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.QUANTITY,INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.PRICE,INCREMENTAL_DATA_PROCESSING.FACT_TRANSACTIONS_STG.STORE_LOCATION);
                    """)
    conn.commit()

    # Fetch the results
    # print("-------------------- Fetch Results from Table ------------------ ")
    # rows = cursor.fetchall()
    # for row in rows:
    #     print(row)
    conn.close()

    # return {
    #     'statusCode': 200,
    #     'body': 'Query executed successfully'
    # }
except Exception as e:
    print("-------" , e)
    # return {
    #     'statusCode': 500,
    #     'body': f'Error: {str(e)}'
    # }

