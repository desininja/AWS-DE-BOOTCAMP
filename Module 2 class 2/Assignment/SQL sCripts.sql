
create database debit_card_spends;
use debit_card_spends;

CREATE TABLE IF NOT EXISTS daily_aggregation (
    customer_id VARCHAR(10),
    debit_card_number VARCHAR(255),
    bank_name VARCHAR(255),
    total_amount_spend FLOAT, 
    PRIMARY KEY (customer_id)
);


select * from daily_aggregation;