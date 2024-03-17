library(DBI)
library(RSQLite)
library(readr)
library(dplyr)
library(lubridate)
library(ggplot2)
library(tidyr)

# Connecting to the database
connection <- RSQLite::dbConnect(RSQLite::SQLite(), "ecomdata.db")

# Retrieve data from the database
customer <- dbGetQuery(connection, "SELECT * FROM customer")
product <- dbGetQuery(connection, "SELECT * FROM product")
supplier <- dbGetQuery(connection, "SELECT * FROM supplier")
category <- dbGetQuery(connection, "SELECT * FROM category")
shipment <- dbGetQuery(connection, "SELECT * FROM shipment")
promotion <- dbGetQuery(connection, "SELECT * FROM promotion")
orders <- dbGetQuery(connection, "SELECT * FROM orders")

# Basic Analysis
## Customer Profile
### The distribution of customer's gender
dbExecute(connection, "
SELECT 
    gender, 
    COUNT(*) AS GenderCount,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM customer), 2) AS Percentage
FROM 
    customer
GROUP BY 
    gender
ORDER BY 
    Percentage DESC;
")

### The Distribution of Customer's Age
dbExecute(connection, "
SELECT 
AgeGroup, 
COUNT(*) AS Count,
CONCAT(ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM customer), 2), '%') AS Percentage
FROM (
  SELECT 
  customer_id,
  CASE
  WHEN age >= 0 AND age < 18 THEN '0-18'
  WHEN age >= 18 AND age < 30 THEN '19-30'
  WHEN age >= 30 AND age < 40 THEN '31-40'
  WHEN age >= 40 AND age < 50 THEN '41-50'
  WHEN age >= 50 AND age < 60 THEN '51-60'
  WHEN age >= 60 AND age < 70 THEN '61-70'
  WHEN age >= 70 THEN '71+'
  ELSE 'Unknown'
  END AS AgeGroup
  FROM customer
) AS AgeCategories
GROUP BY AgeGroup
ORDER BY Count DESC;
")

### The Distribution of Customer's Career (Top 10)
dbExecute(connection, "
SELECT 
career, 
COUNT(*) AS CareerCount,
CONCAT(ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM customer), 2), '%') AS Percentage
FROM 
customer
GROUP BY 
career
ORDER BY 
CareerCount DESC
LIMIT 10;
")

### The Distribution of Customer's Career (Top 10)
dbExecute(connection, "
SELECT 
    career, 
    COUNT(*) AS CareerCount,
    CONCAT(ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM customer), 2), '%') AS Percentage
FROM 
    customer
GROUP BY 
    career
ORDER BY 
    CareerCount DESC
LIMIT 10;
")

### The Distribution of Customer's Geographic Location (Top 10)
dbExecute(connection, "
SELECT 
    address_city, 
    COUNT(*) AS CityCount,
    CONCAT(ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM customer), 2), '%') AS Percentage
FROM 
    customer
GROUP BY 
    address_city
ORDER BY 
    CityCount DESC
LIMIT 10;
")

# Group by city and count the number of customer in each city
customer_city_count <- customer %>%
  group_by(address_city) %>%
  summarise(number_of_customers = n()) %>%
  arrange(desc(number_of_customers))

# Specify the dimensions of the plot
width <- 12 
height <- 8

# Use ggplot to create a bar chart showing the number of customers in each city
g_customer <- ggplot(customer_city_count, 
                     aes(x = reorder(address_city, -number_of_customers), 
                         y = number_of_customers)) +
  geom_col(fill = "dodgerblue") +
  geom_text(aes(label = number_of_customers), 
            position = position_dodge(width = 0.9), vjust = -0.2, 
            size = 3.5) +
  theme(axis.text.x = element_text(angle = 90, hjust = 0.5, vjust = 0), 
        axis.title = element_text(size = 12)) +
  labs(title = "Number of Customers in each City",
       x = "City",
       y = "Number of Customers")
print(g_customer)

# Dynamically generate filename with current date and time
filename <- paste0("geographical distribution of customers_", 
                   format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_customer, width = width, height = height)

### The Current Customer Referral Rate
dbExecute(connection, "
SELECT 
    COUNT(CASE WHEN referred_by != '' AND referred_by IS NOT NULL THEN 1 END) AS Customer_with_Referral,
    COUNT(*) AS total_customer,
    CONCAT(ROUND((COUNT(CASE WHEN referred_by != '' AND referred_by IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)), 2), '%') AS referral_rate
FROM 
    customer;
")

## Product Portfolio
### The Distribution of Product's Review Score (Top 10)
dbExecute(connection, "
SELECT 
    product_name, review_score
FROM 
    product
ORDER BY 
    review_score DESC
LIMIT 10;
")

### The Product Number supplied by Different Suppliers
# Perform an inner join to combine 'product' with 'supplier' on 'supplier_id'
joint_supplier_product <- inner_join(product, supplier, by = "supplier_id")

# Group by supplier_name and count the number of products for each supplier
product_count_by_supplier <- joint_supplier_product %>%
  group_by(supplier_name) %>%
  summarise(number_of_products = n())

# Specify the dimensions of the plot
width <- 12 
height <- 8

# Use ggplot to create a bar chart showing the number of products for each supplier
g_supplier <- ggplot(product_count_by_supplier, aes(x = reorder(supplier_name, -number_of_products), y = number_of_products)) +
  geom_col(fill = "steelblue") + 
  geom_text(aes(label = number_of_products), position = position_dodge(width = 0.9), vjust = -0.2, size = 3.5) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) + 
  labs(title = "Number of Products by Supplier (Descending Order)",
       x = "Supplier Name",
       y = "Number of Products")
print(g_supplier)

# Dynamically generate filename with current date and time
filename <- paste0("the product number supplied by supplier_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_supplier, width = width, height = height)

### The Product's Review Score of Different Suppliers (Top Best 5)
dbExecute(connection, "
SELECT s.supplier_name, ROUND(AVG(p.review_score), 2) AS average_review_score
FROM product p
JOIN supplier s ON p.supplier_id = s.supplier_id
GROUP BY s.supplier_name
ORDER BY average_review_score DESC
LIMIT 5;
")

### The Product's Review Score of Different Suppliers (Top Worst 5)
dbExecute(connection, "
SELECT s.supplier_name, ROUND(AVG(p.review_score), 2) AS average_review_score
FROM product p
JOIN supplier s ON p.supplier_id = s.supplier_id
GROUP BY s.supplier_name
ORDER BY average_review_score ASC
LIMIT 5;
")

### The Top 10 Popular Product
# Perform an inner join to combine 'orders' with 'product' on 'product_id'
joint_order_product <- inner_join(orders, product, by = "product_id")

# Calculate the total quantity sold for each product
product_sales_volume <- joint_order_product %>%
  group_by(product_name) %>%
  summarise(total_quantity_sold = sum(quantity)) # Assuming 'quantity' exists in your orders dataset
# Processing the text of product name
product_sales_volume$product_name <- iconv(product_sales_volume$product_name, "UTF-8", "ASCII//TRANSLIT")

# Choose only the top 10 products based on total quantity sold
top_product_sales_volume <- product_sales_volume %>%
  arrange(desc(total_quantity_sold)) %>%
  slice_head(n = 10)

# Specify the dimensions of the plot
width <- 12
height <- 8

# Use ggplot to create a bar chart showing the total quantity sold for each product
g_topproduct <- ggplot(top_product_sales_volume, aes(x = reorder(product_name, total_quantity_sold), y = total_quantity_sold)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = total_quantity_sold), position = position_dodge(width = 0.9), hjust = -0.2, size = 3.5) +
  coord_flip() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        axis.title = element_text(size = 12)) +
  labs(title = "Top 10 Bestselling Products",
       x = "Product Name",
       y = "Total Quantity Sold")
print(g_topproduct)

# Dynamically generate filename with current date and time
filename <- paste0("top10_products_by_quantity_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_topproduct, width = width, height = height)

## Sales Analysis
### Order Refund Rate
dbExecute(connection, "
SELECT 
    COUNT(CASE WHEN refund_status = 'yes' THEN 1 END) AS refund_orders,
    COUNT(*) AS total_orders,
    CONCAT(ROUND((COUNT(CASE WHEN refund_status = 'yes' THEN 1 END) * 1.0 / COUNT(*)) * 100, 2), '%') AS refund_rate_percentage
FROM (
    SELECT DISTINCT order_id, refund_status
    FROM orders
) AS unique_orders;
")

## Promotion Discount Trend
# Convert Date Format
promotion <- promotion %>%
  mutate(promotion_start_date = as.Date(promotion_start_date),
         promotion_end_date = as.Date(promotion_end_date))

# Generate records for each month that each promotion spans
data_expanded <- promotion %>%
  rowwise() %>%
  mutate(months = list(seq(from = promotion_start_date,
                           to = promotion_end_date,
                           by = "month"))) %>%
  unnest(months) %>%
  mutate(year = year(months), month = month(months)) %>%
  group_by(promotion_id, year, month) %>%
  summarise(promotion_discount_value = mean(promotion_discount_value), .groups = 'drop')

# Calculate the average discount value for each month
average_discounts <- data_expanded %>%
  group_by(year, month) %>%
  summarise(average_discount = mean(promotion_discount_value))

# Specify the dimensions of the plot
width <- 12 
height <- 8

# Visualise the average discount value for different months and years
g_promotionvalue <- ggplot(average_discounts, aes(x = month, y = average_discount, group = year, color = as.factor(year))) +
  geom_line() +
  geom_point() +
  scale_x_continuous(breaks = 1:12, labels = month.abb) +
  geom_text(aes(label = sprintf("%.2f", average_discount)), position = position_dodge(width = 0.9), vjust = -0.2, size = 3.5, color = "black") +
  labs(title = "Average Discount Value by Month and Year",
       x = "Month",
       y = "Average Discount",
       color = "Year")
print(g_promotionvalue)

# Dynamically generate filename with current date and time
filename <- paste0("promotion_discount_trend_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_promotionvalue, width = width, height = height)

## Promotion Count Trend
# Calculate the number of times a promotion appears in each month
promotion_counts <- data_expanded %>%
  count(year, month)

# Visualise the number of times promotions appear in different years and months
g_promotioncount <- ggplot(promotion_counts, aes(x = month, y = n, fill = as.factor(year))) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_x_continuous(breaks = 1:12, labels = month.abb) + 
  geom_text(aes(label = n), position = position_dodge(width = 0.9), vjust = -0.2, size = 3.5) +
  theme(axis.title = element_text(size = 12)) +
  labs(title = "Number of Promotions by Month and Year",
       x = "Month",
       y = "Number of Promotions",
       fill = "Year")
print(g_promotioncount)

# Dynamically generate filename with current date and time
filename <- paste0("promotion_number_trend_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_promotioncount, width = width, height = height)

## Monthly Revenue Trend
# Preprocessed date formats
orders <- orders %>% mutate(order_date = as.Date(order_date))
promotion <- promotion %>%
  mutate(start_date = as.Date(promotion_start_date),
         end_date = as.Date(promotion_end_date),
         promotion_discount_value = if_else(is.na(promotion_discount_value), 0, promotion_discount_value))

# Merge orders with products for pricing information
order_products <- orders %>%
  left_join(product, by = "product_id")

# Make sure there are no missing prices or quantities
order_products <- order_products %>%
  mutate(price = if_else(is.na(price), 0, price),
         quantity = if_else(is.na(quantity), 0, quantity))

# Combine orders, products and promotions to take into account discounts during promotions
order_products_promotions <- order_products %>%
  left_join(promotion, by = "promotion_id") %>%
  mutate(is_promotion = if_else(order_date >= start_date & order_date <= end_date, TRUE, FALSE),
         revenue = price * quantity * if_else(is_promotion, 1 - promotion_discount_value, 1))

# Remove any missing income values generated in the calculation
order_products_promotions <- order_products_promotions %>%
  filter(!is.na(revenue))

# Calculation of gross monthly income
monthly_revenue <- order_products_promotions %>%
  mutate(month = floor_date(order_date, "month")) %>%
  group_by(month) %>%
  summarize(total_revenue = sum(revenue, na.rm = TRUE))

# Visualisation of monthly income
g_monthlyrevenue <- ggplot(monthly_revenue, aes(x = month, y = total_revenue)) +
  geom_line(color = "steelblue") +
  geom_point() +
  scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +  
  geom_text(aes(label = sprintf("%.2f", total_revenue)), position = position_dodge(width = 0.9), vjust = -0.5, size = 3.5) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) + 
  labs(title = "Monthly Revenue", x = "Month", y = "Revenue")
print(g_monthlyrevenue)

# Dynamically generate filename with current date and time
filename <- paste0("monthly_revenue_", 
                   format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_monthlyrevenue, width = width, height = height)

## Monthly Best-Selling Products
# Calculate total monthly revenue per product
monthly_product_revenue <- order_products_promotions %>%
  mutate(month = floor_date(order_date, "month")) %>%
  group_by(month, product_id) %>%
  summarize(total_revenue_product = sum(revenue, na.rm = TRUE))

# Select top earning products per month
best_selling_products_each_month <- monthly_product_revenue %>%
  group_by(month) %>%
  slice_max(total_revenue_product, n = 1) %>%
  ungroup() %>%
  select(month, product_id, total_revenue_product)

best_selling_products_each_month <- merge(best_selling_products_each_month, product[, c("product_id", "product_name")], by = "product_id")

# Visualise the top earning products and their revenues per month
g_bestseller_product_monthly <- ggplot(best_selling_products_each_month, aes(x = month, y = total_revenue_product, fill = product_name)) +
  geom_col(show.legend = FALSE) +  
  geom_text(aes(label = sprintf("%.2f", total_revenue_product)), position = position_dodge(width = 0.9), vjust = -0.5, size = 3.5) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +  
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = "Best-Selling Products by Month", x = "Month", y = "Total Revenue")  

print(g_bestseller_product_monthly)

# Dynamically generate filename with current date and time
filename <- paste0("monthly_bestseller_product_", 
                   format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_bestseller_product_monthly, width = width, height = height)

# Create a table to visualize
best_selling_products_each_month$month <- as.Date(best_selling_products_each_month$month, "%Y-%m-%d")

best_selling_products_each_month$YearMonth <- format(best_selling_products_each_month$month, "%Y-%m")

best_selling_products_each_month <- best_selling_products_each_month %>%
  arrange(YearMonth)

table_to_display <- best_selling_products_each_month %>%
  select(YearMonth, product_name, total_revenue_product) %>%
  rename('Total Revenue' = total_revenue_product)

# Display the table with kable
kable(table_to_display, caption = "Monthly Best-Selling Products", col.names = c("Time", "Product Name", "Total Revenue"))

## Monthly Shipping Efficiency
# Convert dates to Date objects
orders$order_date <- as.Date(orders$order_date)
shipment$shipment_date <- as.Date(shipment$shipment_date)

shipment_unique <- shipment %>% distinct(shipment_id, .keep_all = TRUE)

# Merge orders and shipment data on order_id
combined_data <- merge(orders, shipment, by = "shipment_id")

# Calculate shipping duration in days
combined_data$shipping_duration <- as.numeric(difftime(combined_data$shipment_date, combined_data$order_date, units = "days"))

# Calculate monthly statistics
monthly_stats <- combined_data %>%
  mutate(month = floor_date(order_date, "month")) %>%
  group_by(month) %>%
  summarise(Average_Shipping_Duration = round(mean(shipping_duration),2),
            Min_Shipping_Duration = min(shipping_duration),
            Max_Shipping_Duration = max(shipping_duration))

table_to_display <- monthly_stats %>%
  mutate(Time = format(month, "%Y-%m")) %>% 
  select(Time, Average_Shipping_Duration, Min_Shipping_Duration, Max_Shipping_Duration)

# Display the table with kable
kable(table_to_display, caption = "Monthly Shipping Duration", col.names = c("Time", "Average Duration", "Min Duration", "Max Duration"))

# Visualize the statistics
ggplot(monthly_stats, aes(x = month)) +
  geom_line(aes(y = Average_Shipping_Duration), color = "steelblue", size = 1) +
  geom_text(aes(y = Average_Shipping_Duration, 
                label = sprintf("%.2f", Average_Shipping_Duration)), 
            position = position_dodge(width = 0.9), vjust = -0.5, size = 3.5) +
  scale_x_date(date_labels = "%b %Y", 
               date_breaks = "1 month",
               limits = c(min(monthly_stats$month), max(monthly_stats$month))) + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = "Monthly Shipping Duration",
       x = "Month", y = "Shipping Duration (days)")

# Dynamically generate filename with current date and time
filename <- paste0("monthly_shiping_efficiency_", 
                   format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_shiping_efficiency, width = width, height = height)

## Monthly Delivery Efficiency
# Convert dates to Date objects
shipment$delivery_date <- as.Date(shipment$delivery_date)

shipment_unique <- shipment %>% distinct(shipment_id, .keep_all = TRUE)

# Merge orders and shipment data on order_id
combined_data <- merge(orders, shipment, by = "shipment_id")

# Calculate delivery duration in days
combined_data$delivery_duration <- as.numeric(difftime(combined_data$delivery_date, combined_data$shipment_date, units = "days"))

# Calculate monthly statistics
monthly_stats <- combined_data %>%
  mutate(month = floor_date(order_date, "month")) %>%
  group_by(month) %>%
  summarise(Average_Delivery_Duration = round(mean(delivery_duration),2),
            Min_Delivery_Duration = min(delivery_duration),
            Max_Delivery_Duration = max(delivery_duration))

table_to_display <- monthly_stats %>%
  mutate(Time = format(month, "%Y-%m")) %>% 
  select(Time, Average_Delivery_Duration, Min_Delivery_Duration, Max_Delivery_Duration)

# Display the table with kable
kable(table_to_display, caption = "Monthly Delivery Duration", col.names = c("Time", "Average Duration", "Min Duration", "Max Duration"))

# Visualize the statistics
g_delivery_efficiency <- ggplot(monthly_stats, aes(x = month)) +
  geom_line(aes(y = Average_Delivery_Duration), color = "steelblue", size = 1) +
  geom_text(aes(y = Average_Delivery_Duration, 
                label = sprintf("%.2f", Average_Delivery_Duration)), 
            position = position_dodge(width = 0.9), vjust = -0.5, size = 3.5) +
  scale_x_date(date_labels = "%b %Y", 
               date_breaks = "1 month",
               limits = c(min(monthly_stats$month), max(monthly_stats$month))) + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = "Monthly Delivery Duration",
       x = "Month", y = "Delivery Duration (days)")

# Dynamically generate filename with current date and time
filename <- paste0("monthly_delivery_efficiency_", 
                   format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_shiping_efficiency, width = width, height = height)