
library(readr)
library(RSQLite)

connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/database.db")


dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS customers_ak ( 

    customer_id INT PRIMARY KEY, 
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL, 
    email_id VARCHAR(50), 
    gender VARCHAR(50), 
    telephone_number VARCHAR(20), 
    shipping_address VARCHAR(50)
   
);
")

RSQLite::dbListTables(connection)

# Load data from rough.csv file into customers_ak table

customer_data <- readr::read_csv("data_upload/rough.csv")
RSQLite::dbWriteTable(connection, "customers_ak", customer_data, append = TRUE, row.names = FALSE)

dbDisconnect(connection)
