library(readr)
library(DBI)
library(RSQLite)

# List of files 
all_files <- list.files("data_upload/")
all_files

#Cheking number of rows and columns in each file
#listing all files 
all_files <- list.files("data_upload/")

#Creating a loop to read all files
for (variable in all_files) {
  filepath <- paste0("data_upload/",variable)
  file_contents <- readr::read_csv(filepath)
  
  number_of_rows <- nrow(file_contents)
  number_of_columns <- ncol(file_contents)
  
  #Printing the number of rows and columns in each file  
  print(paste0("The file: ",variable,
               " has: ",
               format(number_of_rows,big.mark = ","),
               " rows and ",
               number_of_columns," columns"))
  number_of_rows <- nrow(file_contents)
  print(paste0("Checking for file: ",variable))
  
  #Printing True if the first column is the primary key column else printing False  
  print(paste0(" is ",nrow(unique(file_contents[,1]))==number_of_rows))
}


#Creating database connection
# Create a connection
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"ecomdata.db")
RSQLite::dbListTables(connection)

##Creating table Schema for all tables
#Creating Customer Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS customer ( 
  
  customer_id INT PRIMARY KEY, 
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL, 
  email VARCHAR(50) NOT NULL, 
  gender VARCHAR(50) NOT NULL,
  age INT NOT NULL,
  career VARCHAR(50) NOT NULL,
  customer_phone VARCHAR(20) NOT NULL, 
  address_country VARCHAR(50) NOT NULL,
  address_zipcode VARCHAR(20) NOT NULL,
  address_city VARCHAR(20) NOT NULL,
  address_street VARCHAR(50) NOT NULL,
  referred_by VARCHAR(20) NULL
  
);
")

#Creating Supplier Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS supplier ( 
  
  supplier_id VARCHAR(50) PRIMARY KEY, 
  supplier_name VARCHAR(50) NOT NULL
);
")


#Creating Category Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS category ( 
  
  category_id VARCHAR(20) PRIMARY KEY, 
  category_name VARCHAR(50) NOT NULL
);
")

#Creating Promotion Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS promotion ( 
  
  promotion_id VARCHAR(20) PRIMARY KEY, 
  promotion_name VARCHAR(20) NOT NULL,
  promotion_start_date DATE NOT NULL,
  promotion_end_date DATE NOT NULL,
  promotion_discount_value FLOAT NOT NULL
);
")

#Creating Product Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS product ( 
  
  product_id VARCHAR(20) PRIMARY KEY, 
  category_id VARCHAR(20) NOT NULL,
  supplier_id VARCHAR(20) NOT NULL, 
  promotion_id VARCHAR(20) NULL, 
  product_name VARCHAR(20) NOT NULL,
  price INT NOT NULL,
  quantity_stock INT NOT NULL,
  quantity_supplied INT NOT NULL, 
  review_score FLOAT NOT NULL,
  FOREIGN KEY ('category_id') REFERENCES category('category_id'), 
  FOREIGN KEY ('supplier_id') REFERENCES supplier('supplier_id'), 
  FOREIGN KEY ('promotion_id') REFERENCES promotion('promotion_id')     
);
")

#Creating Shipment Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS shipment ( 
  
  shipment_id VARCHAR(20) PRIMARY KEY, 
  shipment_status VARCHAR(20) NOT NULL
);
")

#Creating Orders Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS orders ( 
  
  order_id VARCHAR(20) NOT NULL, 
  product_id VARCHAR(20) NOT NULL,
  customer_id VARCHAR(20) NOT NULL,
  shipment_id VARCHAR(20) NOT NULL,
  order_status VARCHAR(20) NOT NULL,
  refund_status VARCHAR(20) NOT NULL,
  quantity INT NOT NULL,
  /*quantity INT NOT NULL,
  refund_status BOOLEAN NOT NULL, */
    FOREIGN KEY ('customer_id') REFERENCES customer('customer_id'), 
  FOREIGN KEY ('product_id') REFERENCES product('product_id'),
  FOREIGN KEY ('shipment_id') REFERENCES shipment('shipment_id')
  PRIMARY KEY (order_id,product_id,customer_id,shipment_id)
);
")


#Listing tables from the database that we created
RSQLite::dbListTables(connection)


#Loading data from csv to database tables
load_data_to_db <- function(filename, db_table_name, connection) {
  data <- read.csv(filename)
  RSQLite::dbWriteTable(connection, db_table_name, data, append = TRUE, row.names = FALSE)
}

load_data_to_db("data_upload/customer.csv", "customer",connection)
load_data_to_db("data_upload/supplier.csv", "supplier", connection)
load_data_to_db("data_upload/category.csv", "category", connection)
load_data_to_db("data_upload/shipment.csv", "shipment", connection)
load_data_to_db("data_upload/promotion.csv", "promotion", connection)
load_data_to_db("data_upload/product.csv", "product", connection)
load_data_to_db("data_upload/orders.csv", "orders", connection)

#Storing data in RDS format
all_files <- list.files("data_upload/")
for (variable in all_files) {
  filepath <- paste0("data_upload/",variable)
  file_contents <- readr::read_csv(filepath)
  table_name <- gsub(".csv","",variable)
  save(file_contents,file = paste0("rdadata/",table_name,".rds"))
}




#Disconnecting from Database
RSQLite::dbDisconnect(connection)
