library(readr)
library(DBI)
library(RSQLite)
library(dplyr)


# Creating database connection
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"ecomdata.db")


# Creating table Schema for all tables

#Creating Customer Table

RSQLite::dbExecute(connection,"

CREATE TABLE IF NOT EXISTS customer ( 

    customer_id VARCHAR(10) PRIMARY KEY, 
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL, 
    email VARCHAR(50) NOT NULL, 
    gender VARCHAR(20) NOT NULL,
    age INT NOT NULL,
    career VARCHAR(50) NULL,
    customer_phone VARCHAR(20) NOT NULL, 
    address_country VARCHAR(50) NOT NULL,
    address_zipcode VARCHAR(20) NOT NULL,
    address_city VARCHAR(20) NOT NULL,
    address_street VARCHAR(50) NOT NULL,
    referred_by VARCHAR(10) NULL
    
);
")


#Creating Category Table

RSQLite::dbExecute(connection,"

CREATE TABLE IF NOT EXISTS category ( 

    category_id VARCHAR(10) PRIMARY KEY, 
    category_name VARCHAR(50) NOT NULL

);

")


#Creating Supplier Table
  
RSQLite::dbExecute(connection,"
CREATE TABLE IF NOT EXISTS supplier ( 

    supplier_id VARCHAR(10) PRIMARY KEY, 
    supplier_name VARCHAR(50) NOT NULL
    
);

")


#Creating Promotion Table

RSQLite::dbExecute(connection,"

CREATE TABLE IF NOT EXISTS promotion ( 

    promotion_id VARCHAR(10) PRIMARY KEY, 
    promotion_name VARCHAR(20) NOT NULL,
    promotion_start_date DATE NOT NULL,
    promotion_end_date DATE NOT NULL,
    promotion_discount_value FLOAT NOT NULL

);

")


#Creating Shipment Table

RSQLite::dbExecute(connection,"

CREATE TABLE IF NOT EXISTS shipment ( 

    shipment_id VARCHAR(10) PRIMARY KEY,
    shipment_date DATE NOT NULL,
    delivery_date DATE NOT NULL
);

")



#Creating Product Table

RSQLite::dbExecute(connection,"

CREATE TABLE IF NOT EXISTS product ( 

    product_id VARCHAR(10) PRIMARY KEY, 
    category_id VARCHAR(10) NOT NULL,
    supplier_id VARCHAR(10) NOT NULL, 
    promotion_id VARCHAR(10) NULL, 
    product_name VARCHAR(20) NOT NULL,
    price FLOAT NOT NULL,
    quantity_stock INT NOT NULL,
    quantity_supplied INT NOT NULL, 
    review_score FLOAT NOT NULL,
    FOREIGN KEY ('category_id') REFERENCES category('category_id'), 
    FOREIGN KEY ('supplier_id') REFERENCES supplier('supplier_id'), 
    FOREIGN KEY ('promotion_id') REFERENCES promotion('promotion_id')     
   
);

")

#Creating Orders Table

RSQLite::dbExecute(connection,"

CREATE TABLE IF NOT EXISTS orders ( 

    order_id VARCHAR(20) NOT NULL, 
    customer_id VARCHAR(10) NOT NULL,
    product_id VARCHAR(10) NOT NULL,
    shipment_id VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    refund_status VARCHAR(20) NOT NULL,
    order_date DATE NOT NULL,
    FOREIGN KEY ('customer_id') REFERENCES customer('customer_id'), 
    FOREIGN KEY ('product_id') REFERENCES product('product_id'),
    FOREIGN KEY ('shipment_id') REFERENCES shipment('shipment_id')
    PRIMARY KEY (order_id,product_id,customer_id,shipment_id)


);

")


# Listing tables from the database that we created
RSQLite::dbListTables(connection)


# List of CSV Files
all_files <- list.files("data_upload/")
all_files


# Creating a loop to read on all files

# Number of rows and columns in each file

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



# Structure of data

for (variable in all_files) {
  filepath <- paste0("data_upload/",variable)
  file_contents <- readr::read_csv(filepath)
  str_data <-str(file_contents)
  print(paste0(str_data,"Sructure of the file ", variable))
}


# Number of columns and their column names

for (variable in all_files) {
  filepath <- paste0("data_upload/",variable)
  file_contents <- readr::read_csv(filepath)
  column_names <-colnames(file_contents)
  print(paste0("File ", variable, " has column as ",column_names))
}


# Checking unique id column as the primary key in each table

for (variable in all_files) {
  filepath <- paste0("data_upload/", variable)
  file_contents <- readr::read_csv(filepath)
  primary_key <- nrow(unique(file_contents[,1])) == nrow(file_contents)
  print(paste0("Primary key of ", variable, " is ", primary_key))
}



#Data Validation

#Validation for customer data 
#Establishing Connection 

fetch_existing_customer_ids <- function(connection) {
  query <- "SELECT DISTINCT customer_id FROM customer"
  existing_ids <- dbGetQuery(connection, query)$customer_id
  return(existing_ids)
  
}

#Customer data validation and Reffered by referencial integrity
validate_and_prepare_customer_data <- function(data, existing_ids) {
  
  #Validation for customer ID  
  customer_id_check <- grepl("^CUST[0-9]{6}$", data$customer_id)
  data <- data[customer_id_check, ]
  
  #Validation for email
  email_check <- grepl("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", data$email)
  data<- data[email_check, ]
  
  #Validation for gender
  gender_check <- c("male","female","Female","Male")
  data<- data[data$gender %in% gender_check, ]
  
  #Validation for age
  age_check <- 1:100
  data <- data[data$age %in% age_check, ]
  
  #Validation for phone number
  phone_check <- grepl("^\\+44 \\d{10}$", data$customer_phone)
  data <- data[phone_check, ]
  
  #Validation for zip code
  zipcode_check <- grepl("^\\w{3} \\w{3}$", data$address_zipcode)
  data <- data[zipcode_check, ]
  
  #Reffered by check
  unique_customer_ids <- unique(c(data$customer_id, existing_ids))
  #Validate 'referred by' IDs
  valid_referral_flags <- (data$referred_by == "") | data$referred_by %in% unique_customer_ids
  data <- data[valid_referral_flags, ]
  
  return(data)
}

#Fetch existing customer IDs from the database
existing_customer_ids <- fetch_existing_customer_ids(connection)

customer_file_paths <- list.files(path = "data_upload", pattern = "customer.*\\.csv$", full.names = TRUE)
#Initialising empty dataframe
customer_possible_data <- data.frame()  

customer_primary_key <- "customer_id"

#Read each customer CSV file and check for the existence of the primary key in the database before appending
for (file_path in customer_file_paths) {
  cat("Starting processing file:", file_path, "\n")
  #Read the current file
  customer_data <- read.csv(file_path)
  
  #Iterate through each row of the file
  for (i in seq_len(nrow(customer_data))) {
    new_record <- customer_data[i, ]
    primary_key_value <- new_record[[customer_primary_key]]
    conditions <- paste(customer_primary_key, "=", paste0("'", primary_key_value, "'"))
    
    #Check if a record with the same primary key exists in the database
    record_exists_query <- paste("SELECT COUNT(*) FROM customer WHERE", conditions)
    record_exists_result <- dbGetQuery(connection, record_exists_query)
    record_exists <- record_exists_result[1, 1] > 0
    
    if(record_exists) {
      cat("Record with primary key", primary_key_value, "already exists in the database.\n")
    }  
    if (!record_exists) {
      #Check if the primary key value of the new record is unique in the temporary dataframe
      if (!primary_key_value %in% customer_possible_data[[customer_primary_key]]) {
        customer_possible_data <- rbind(customer_possible_data, new_record)
      }
    }
    
    cat("Finished processing file:", file_path, "\n")
    
  }
  cat("Starting validation for new records.\n")
  customer_possible_data <- validate_and_prepare_customer_data(customer_possible_data, existing_customer_ids)
  cat("Validation completed for new records.\n")
}


if (nrow(customer_possible_data) > 0) 
{
  cat("Starting to insert validated data into the database. Number of records: ", nrow(customer_possible_data), "\n")
  
  #Ingesting prepared data to our database
  dbWriteTable(connection, name = "customer", value = customer_possible_data, append = TRUE, row.names = FALSE)
  cat("Data insertion completed successfully.\n")
} else 
{
  cat("No valid customer data to insert into the database.\n")
}




#Validations for category data file

validate_and_prepare_category_data <- function(data) {
  
  # Validation for category ID
  category_id_check <- grepl("^[A-Za-z0-9]{10}$", data$category_id)
  data <- data[category_id_check,]
  
  return(data) 
}

# Fetch existing category IDs from the database

category_file_paths <- list.files(path = "data_upload", pattern = "category.*\\.csv$", full.names = TRUE)

# Define the primary key column for the category table
category_primary_key <- "category_id"

#Initialising empty data frame
category_possible_data <- data.frame() 

# Read each category CSV file and check for the existence of the primary key in the database before appending
for (file_path in category_file_paths) {
  
  cat("Starting processing file:", file_path, "\n")
  
  # Read the current file
  category_data <- readr::read_csv(file_path)
  
  # Iterate through each row of the file
  for (i in seq_len(nrow(category_data))) {
    new_record <- category_data[i, ]
    primary_key_value <- new_record[[category_primary_key]]
    conditions <- paste(category_primary_key, "=", paste0("'", primary_key_value, "'"))
    
    # Check if a record with the same primary key exists in the database
    record_exists_query <- paste("SELECT COUNT(*) FROM category WHERE", conditions)
    record_exists_result <- dbGetQuery(connection, record_exists_query)
    record_exists <- record_exists_result[1, 1] > 0
    
    if(record_exists) {
      cat("Record with primary key", primary_key_value, "already exists in the database.\n")
    }  
    if (!record_exists) {
      # Check if the primary key value of the new record is unique in the temporary dataframe
      if (!primary_key_value %in% category_possible_data[[category_primary_key]]) {
        category_possible_data <- rbind(category_possible_data, new_record)
      }
    }
    
    cat("Finished processing file:", file_path, "\n")
    
  }
  
}
cat("Starting validation for new records.\n")
category_possible_data <- validate_and_prepare_category_data(category_possible_data)
cat("Validation completed for new records.\n")

if (nrow(category_possible_data) > 0) {
  cat("Starting to insert validated data into the database. Number of records: ", nrow(category_possible_data), "\n")
  
  # Ingesting prepared data to our database
  dbWriteTable(connection, name = "category", value = category_possible_data, append = TRUE, row.names = FALSE)
  cat("Data insertion completed successfully.\n")
} else 
{
  cat("No valid category data to insert into the database.\n")
}



## Validations for supplier data


validate_and_prepare_supplier_data <- function(data) {
  # Validation for supplier ID
  supplier_id_check <- grepl("^[A-Za-z0-9]{10}$", data$supplier_id)
  data <- data[supplier_id_check,]
  
  return(data) 
}

# Fetch existing supplier IDs from the database

supplier_file_paths <- list.files(path = "data_upload", pattern = "supplier.*\\.csv$", full.names = TRUE)

# Define the primary key column for the supplier table
supplier_primary_key <- "supplier_id"

#Initialising empty data frame
supplier_possible_data <- data.frame() 

# Read each supplier CSV file and check for the existence of the primary key in the database before appending
for (file_path in supplier_file_paths) {
  
  cat("Starting processing file:", file_path, "\n")
  
  # Read the current file
  supplier_data <- readr::read_csv(file_path)
  
  # Iterate through each row of the file
  for (i in seq_len(nrow(supplier_data))) {
    new_record <- supplier_data[i, ]
    primary_key_value <- new_record[[supplier_primary_key]]
    conditions <- paste(supplier_primary_key, "=", paste0("'", primary_key_value, "'"))
    
    # Check if a record with the same primary key exists in the database
    record_exists_query <- paste("SELECT COUNT(*) FROM supplier WHERE", conditions)
    record_exists_result <- dbGetQuery(connection, record_exists_query)
    record_exists <- record_exists_result[1, 1] > 0
    
    if(record_exists) {
      cat("Record with primary key", primary_key_value, "already exists in the database.\n")
    }  
    if (!record_exists) {
      # Check if the primary key value of the new record is unique in the temporary dataframe
      if (!primary_key_value %in% supplier_possible_data[[supplier_primary_key]]) {
        supplier_possible_data <- rbind(supplier_possible_data, new_record)
      }
    }
    
    cat("Finished processing file:", file_path, "\n")
    
  }
  
  cat("Starting validation for new records.\n")
  supplier_possible_data <- validate_and_prepare_supplier_data(supplier_possible_data)
  cat("Validation completed for new records.\n")
}

if (nrow(supplier_possible_data) > 0) {
  cat("Starting to insert validated data into the database. Number of records: ", nrow(supplier_possible_data), "\n")
  # Digesting prepared data to our database
  dbWriteTable(connection, name = "supplier", value = supplier_possible_data, append = TRUE, row.names = FALSE)
  cat("Data insertion completed successfully.\n")
} else 
{
  cat("No valid supplier data to insert into the database.\n")
}



#Validations for promotion data

validate_and_prepare_promotion_data <- function(data) {
  
  # Validation for promotion ID
  promotion_id_check <- grepl("^[A-Za-z0-9]{10}$", data$promotion_id)
  data <- data[promotion_id_check, ]
  
  #Checking for the validation of the promotion_start_date and promotion_end_date in the promotion table
  date_check <- !is.na(as.Date(data$promotion_start_date, format = "%d-%m-%Y")) &
    !is.na(as.Date(data$promotion_end_date, format = "%d-%m-%Y")) &
    as.Date(data$promotion_start_date, format = "%d-%m-%Y") < as.Date(data$promotion_end_date, format = "%d-%m-%Y")
  
  #Check for the validation of the promotion_start_date and promotion_end_date in the promotion   table.
  #promotion_start_date and promotion_end_data should be in correct form for eg 12/11/2023
  date_format <- "%d-%m-%Y"
  date_check <- !is.na(as.Date(data$promotion_start_date, format = date_format)) &
    !is.na(as.Date(data$promotion_end_date, format = date_format)) &
    as.Date(data$promotion_start_date, format = date_format) < as.Date(data$promotion_end_date, format = date_format)
  data <- data[date_check,]
  
  
  #Check for the validation of the column promotion_discount_value in the promotion table.
  #promotion_discount_value should be <1
  
  discount_value_check <- !is.na(data$promotion_discount_value) &
    is.numeric(data$promotion_discount_value) &
    data$promotion_discount_value < 1
  data <- data[discount_value_check,]
  return(data)
}


# Fetch existing promotion IDs from the database

promotion_file_paths <- list.files(path = "data_upload", pattern = "promotion.*\\.csv$", full.names = TRUE)

# Define the primary key column for the promotion table
promotion_primary_key <- "promotion_id"

#Initialising empty data frame
promotion_possible_data <- data.frame() 


# Read each promotion CSV file and check for the existence of the primary key in the database before appending
for (file_path in promotion_file_paths) {
  
  cat("Starting processing file:", file_path, "\n")
  
  # Read the current file
  promotion_data <- readr::read_csv(file_path)
  
  # Iterate through each row of the file
  for (i in seq_len(nrow(promotion_data))) {
    new_record <- promotion_data[i, ]
    primary_key_value <- new_record[[promotion_primary_key]]
    conditions <- paste(promotion_primary_key, "=", paste0("'", primary_key_value, "'"))
    
    # Check if a record with the same primary key exists in the database
    record_exists_query <- paste("SELECT COUNT(*) FROM promotion WHERE", conditions)
    record_exists_result <- dbGetQuery(connection, record_exists_query)
    record_exists <- record_exists_result[1, 1] > 0
    
    if(record_exists) {
      cat("Record with primary key", primary_key_value, "already exists in the database.\n")
    }  
    if (!record_exists) {
      # Check if the primary key value of the new record is unique in the temporary dataframe
      if (!primary_key_value %in% promotion_possible_data[[promotion_primary_key]]) {
        promotion_possible_data <- rbind(promotion_possible_data, new_record)
      }
    }
    
    cat("Finished processing file:", file_path, "\n")
    
  }
}
    cat("Starting validation for new records.\n")
    promotion_possible_data <- validate_and_prepare_promotion_data(promotion_possible_data)
    cat("Validation completed for new records.\n")


if (nrow(promotion_possible_data) > 0) 
{
  cat("Starting to insert validated data into the database. Number of records: ", nrow(promotion_possible_data), "\n")
  # Digesting prepared data to our database
  dbWriteTable(connection, name = "promotion", value = promotion_possible_data, append = TRUE, row.names = FALSE)
  cat("Data insertion completed successfully.\n")
} else 
{
  cat("No valid promotion data to insert into the database.\n")
}



#Validations for shipment data

    validate_and_prepare_shipment_data <- function(data) {
      # Validation for shipment ID
      shipment_id_check <- grepl("^SHIP[0-9]{6}$", data$shipment_id)
      data <- data[shipment_id_check,]
      
      # Validation for shipment_date and delivery_date format
      date_format_check <- !is.na(as.Date(data$shipment_date, format = "%d-%m-%Y")) &
        !is.na(as.Date(data$delivery_date, format = "%d-%m-%Y"))
      
      data <- data[date_format_check,]
      
      #Validation for loogical order of shipment and delivery dates
      logical_date_order_check <- as.Date(data$shipment_date) < as.Date(data$delivery_date)
      data <- data[logical_date_order_check,]
      return(data) 
    }
    
    # Fetch existing shipment IDs from the database
    
    shipment_file_paths <- list.files(path = "data_upload", pattern = "shipment.*\\.csv$", full.names = TRUE)
    
    # Define the primary key column for the shipment table
    shipment_primary_key <- "shipment_id"
    
    #Initialising empty data frame
    shipment_possible_data <- data.frame() 
    
    # Read each shipment CSV file and check for the existence of the primary key in the database before appending
    for (file_path in shipment_file_paths) {
      
      cat("Starting processing file:", file_path, "\n")
      
      # Read the current file
      shipment_data <- readr::read_csv(file_path)
      
      # Iterate through each row of the file
      for (i in seq_len(nrow(shipment_data))) {
        new_record <- shipment_data[i, ]
        primary_key_value <- new_record[[shipment_primary_key]]
        conditions <- paste(shipment_primary_key, "=", paste0("'", primary_key_value, "'"))
        
        # Check if a record with the same primary key exists in the database
        record_exists_query <- paste("SELECT COUNT(*) FROM shipment WHERE", conditions)
        record_exists_result <- dbGetQuery(connection, record_exists_query)
        record_exists <- record_exists_result[1, 1] > 0
        
        if(record_exists) {
          cat("Record with primary key", primary_key_value, "already exists in the database.\n")
        }  
        if (!record_exists) {
          # Check if the primary key value of the new record is unique in the temporary dataframe
          if (!primary_key_value %in% shipment_possible_data[[shipment_primary_key]]) {
            shipment_possible_data <- rbind(shipment_possible_data, new_record)
          }
        }
        
        cat("Finished processing file:", file_path, "\n")
        
      }
    }
    cat("Starting validation for new records.\n")
    shipment_possible_data <- validate_and_prepare_shipment_data(shipment_possible_data)
    cat("Validation completed for new records.\n")
    
    if (nrow(shipment_possible_data) > 0) {
      cat("Starting to insert validated data into the database. Number of records: ", nrow(shipment_possible_data), "\n")
      # Digesting prepared data to our database
      dbWriteTable(connection, name = "shipment", value = shipment_possible_data, append = TRUE, row.names = FALSE)
      cat("Data insertion completed successfully.\n")
    } else {
      cat("No valid shipment data to insert into the database.\n")
    }



#Validations for product data

    validate_and_prepare_product_data <- function(data) {
      # Validation for product ID
      product_id_check <- grepl("^[A-Za-z0-9]{10}$", data$product_id)
      data <- data[product_id_check, ]
      # Performing validation checks here
      data <- data[data$review_score >= 1 & data$review_score <= 5, ]
      
      return(data)
    }
    
    
    # Fetch existing product IDs from the database
    
    product_file_paths <- list.files(path = "data_upload", pattern = "product.*\\.csv$", full.names = TRUE)
    
    # Define the primary key column for the product table
    product_primary_key <- "product_id"
    
    #Initialising empty data frame
    product_possible_data <- data.frame() 
    
    
    # Read each product CSV file and check for the existence of the primary key in the database before appending
    for (file_path in product_file_paths) {
      
      cat("Starting processing file:", file_path, "\n")
      
      # Read the current file
      product_data <- readr::read_csv(file_path)
      
      # Iterate through each row of the file
      for (i in seq_len(nrow(product_data))) {
        new_record <- product_data[i, ]
        primary_key_value <- new_record[[product_primary_key]]
        conditions <- paste(product_primary_key, "=", paste0("'", primary_key_value, "'"))
        
        # Check if a record with the same primary key exists in the database
        record_exists_query <- paste("SELECT COUNT(*) FROM product WHERE", conditions)
        record_exists_result <- dbGetQuery(connection, record_exists_query)
        record_exists <- record_exists_result[1, 1] > 0
        
        if(record_exists) {
          cat("Record with primary key", primary_key_value, "already exists in the database.\n")
        }  
        if (!record_exists) {
          # Check if the primary key value of the new record is unique in the temporary dataframe
          if (!primary_key_value %in% product_possible_data[[product_primary_key]]) {
            product_possible_data <- rbind(product_possible_data, new_record)
          }
        }
        
        cat("Finished processing file:", file_path, "\n")
        
      }
    }
        cat("Starting validation for new records.\n")
        product_possible_data <- validate_and_prepare_product_data(product_possible_data)
        cat("Validation completed for new records.\n")
    
    if (nrow(product_possible_data) > 0) 
    {
      cat("Starting to insert validated data into the database. Number of records: ", nrow(product_possible_data), "\n")
      # Digesting prepared data to our database
      dbWriteTable(connection, name = "product", value = product_possible_data, append = TRUE, row.names = FALSE)
      cat("Data insertion completed successfully.\n")
    } else 
    {
      cat("No valid product data to insert into the database.\n")
    }
    

# Validation for orders data

        validate_and_prepare_orders_data <- function(data){
          # Checking format of order id  
          order_id_check <- grepl("^ORDER[0-9]{9}$", data$order_id)
          data <- data[order_id_check, ]
          
          customer_id_check <- grepl("^CUST[0-9]{6}$", data$customer_id)
          data <- data[customer_id_check, ]
          
          product_id_check <- grepl("^[A-Za-z0-9]{10}$", data$product_id)
          data <- data[product_id_check, ]
          
          shipment_id_check <- grepl("^SHIP[0-9]{6}$", data$shipment_id)
          data <- data[shipment_id_check,]
          
          # Validation for order_date format
          order_date_format_check <- !is.na(as.Date(data$order_date, format = "%d-%m-%Y"))
          data <- data[order_date_format_check,]
          
          
          return(data) 
        }
        
        
        orders_file_paths <- list.files(path = "data_upload", pattern = "orders.*\\.csv$", full.names = TRUE)
        orders_possible_data <- data.frame()  
        
        
        # Read each orders CSV file and check for the existence of the composite primary key in the database before appending
        for (file_path in orders_file_paths) {
          orders_data <- readr::read_csv(file_path)
          
          # Iterate through each row of the file
          for (i in seq_len(nrow(orders_data))) {
            new_record <- orders_data[i, ]
            # primary_key_value <- new_record[[orders_primary_key]]
            # Construct the condition to check the composite primary key (order_id, product_id, customer_id, shipment_id)
            conditions <- sprintf("order_id = '%s' AND product_id = '%s' AND customer_id = '%s' AND shipment_id = '%s'", 
                                  new_record$order_id, new_record$product_id, new_record$customer_id, new_record$shipment_id)
            
            # Check if a record with the same composite primary key exists in the database
            record_exists_query <- paste("SELECT COUNT(*) FROM orders WHERE", conditions)
            record_exists_result <- dbGetQuery(connection, record_exists_query)
            record_exists <- record_exists_result[1, 1] > 0
            
            
            if(!record_exists) {
              # Construct a unique identifier for the composite primary key
              composite_key <- paste(new_record$order_id, new_record$product_id, new_record$customer_id, new_record$shipment_id, sep = "-")
              
              # Check if the composite primary key is unique in the temporary dataframe
              existing_keys <- sapply(1:nrow(orders_possible_data), function(i) {
                paste(orders_possible_data[i, "order_id"], orders_possible_data[i, "product_id"], orders_possible_data[i, "customer_id"], orders_possible_data[i, "shipment_id"], sep = "-")
              })
              
              if (!composite_key %in% existing_keys) {
                orders_possible_data <- rbind(orders_possible_data, new_record)
              } else {
                cat("Record with composite primary key already exists in temporary data.\n")
              }
            } else {
              cat("Record with composite primary key already exists in the database.\n")
            }
          }
        }
        orders_possible_data <- validate_and_prepare_orders_data(orders_possible_data)
        
        if (nrow(orders_possible_data) > 0) {
          cat("Starting to insert validated data into the database. Number of records: ", nrow(orders_possible_data), "\n")
          
          # Ingesting prepared data to our database
         
           dbWriteTable(connection, name = "orders", value = orders_possible_data, append = TRUE, row.names = FALSE)
          cat("Data insertion completed successfully.\n")
        } else {
          cat("No valid orders data to insert into the database.\n")
        }



#Disconnecting from Database
RSQLite::dbDisconnect(connection)


