def MyTransform (glueContext, dfc) -> DynamicFrame:
    import pymysql
    pymysql.install_as_MySQLdb()
    import MySQLdb
    
    dbname = "debit_card_spends"
    table_name = "daily_aggregation"
    
    user_name = 'mysql_admin' 
    password =  '123456789' 
    host = 'debit-card-customer.cl80ocesmj0s.us-east-1.rds.amazonaws.com' 
    port = 3306  
    
    mysql_client = MySQLdb.connect(host=host, user=user_name, passwd=password, port=port, database=dbname)
    
    cur = mysql_client.cursor(MySQLdb.cursors.DictCursor)
    query = "TRUNCATE TABLE {0}.{1}".format(dbname,table_name)
    cur.execute(query)
    mysql_client.commit()
    
    # new_df = dfc.select(list(dfc.keys())[0]).toDF()
    # new_custom_dyc = DynamicFrame.fromDF(new_df,glueContext,"new_df_data")
    
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df_ = DynamicFrame.fromDF(df, glueContext, "new_df")
    return(DynamicFrameCollection({"CustomTransform0": df_}, glueContext))