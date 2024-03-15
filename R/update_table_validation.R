#Importing Libraries
library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

#Connecting to the database
ecomdata <- RSQLite::dbConnect(RSQLite::SQLite(), "ecomdata.db")


#Listing the paths of the CSV Files
data_file_paths <- list(
  "category" = list.files(path = "data_upload", pattern = "category.*\\.csv$", full.names = TRUE),
  "supplier" = list.files(path = "data_upload", pattern = "supplier.*\\.csv$", full.names = TRUE),
  "customer" = list.files(path = "data_upload", pattern = "customer*\\.csv$", full.names = TRUE),
  "promotion" = list.files(path = "data_upload", pattern = "promotion.*\\.csv$", full.names = TRUE),
  "shipment" = list.files(path = "data_upload", pattern = "shipment.*\\.csv$", full.names = TRUE),
  "product" = list.files(path = "data_upload", pattern = "product.*\\.csv$", full.names = TRUE),
  "orders" = list.files(path = "data_upload", pattern = "orders.*\\.csv$", full.names = TRUE)
)

#listing each data category to its primary key column
##orders table is an associative table and thus we have a composite primary key

ecom_tables <- list(
  "category" = "category_id",
  "supplier" = "supplier_id",
  "customer" = "customer_id",
  "promotion" = "promotion_id",
  "shipment" = "shipment_id",
  "product" = "product_id",
  "orders" = c("order_id", "product_id", "customer_id","shipment_id")
)


#Checking the primary key from each table and if it already exists it will not append the data otherwise it will
for (table_name in names(ecom_tables)) {
  for (file_path in data_file_paths[[table_name]]) {
    table_data <- read_csv(file_path) 
    for (i in seq_along(table_data)) {
      new_record <- table_data[i, ]
      primary_key_columns <- ecom_tables[[table_name]]
      primary_key_values <- new_record[primary_key_columns]
      conditions <- paste(primary_key_columns, "=", paste0("'", primary_key_values, "'"), collapse = " AND ")
      
      record_exists <- dbGetQuery(ecomdata, paste("SELECT COUNT(*) FROM", table_name, "WHERE", conditions))
      
      if (record_exists == 0) {
        tryCatch({
          RSQLite::dbAppendTable(ecomdata, table_name, new_record, overwrite = FALSE, append = TRUE)
        }, error = function(e) {
          print(paste("Error inserting record with primary key", paste(primary_key_values, collapse = ", "), "into table", table_name))
          print(e)
        })
      }
    }
  }
}
