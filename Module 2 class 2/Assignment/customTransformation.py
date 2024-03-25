def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    import json
    import boto3
    from botocore.exceptions import ClientError
    import pymysql
    
    dbname = "debit_card_spends"
    table_name = "daily_aggregation"
    
    
    secret_name = "mysql_database_creds"
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
    secret_json = json.loads(secret)
    
    user_name = secret_json["username"]
    password = secret_json["password"]
    host = secret_json["host"]
    port = secret_json["port"]
    
    conn =  pymysql.connect(host=host, user=user_name, passwd=password, port=port, database=dbname)
    cur = conn.cursor()
    query = "TRUNCATE TABLE {0}.{1}".format(dbname,table_name)
    cur.execute(query)
    conn.commit()
    