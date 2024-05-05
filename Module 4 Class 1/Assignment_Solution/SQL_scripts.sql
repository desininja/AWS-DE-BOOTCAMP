CREATE TABLE incremental_data_processing.fact_transactions_stg (
    transaction_id VARCHAR(255)  ENCODE lzo,
    transaction_date DATE ENCODE bytedict,
    product_id VARCHAR(255) ENCODE lzo,
    quantity INT ENCODE delta,
    price DECIMAL(10,2) ENCODE delta,
    store_location VARCHAR(255) ENCODE lzo
)
DISTSTYLE KEY
DISTKEY (transaction_id)
SORTKEY (transaction_id , product_id);

CREATE TABLE incremental_data_processing.fact_transactions  (
    transaction_id VARCHAR(255)  ENCODE lzo,
    transaction_date DATE ENCODE bytedict,
    product_id VARCHAR(255) ENCODE lzo,
    quantity INT ENCODE delta,
    price DECIMAL(10,2) ENCODE delta,
    store_location VARCHAR(255) ENCODE lzo
)
DISTSTYLE KEY
DISTKEY (transaction_id)
SORTKEY (transaction_id , product_id);