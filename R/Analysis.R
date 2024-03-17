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
            size = 3.5, family = "Times") +
  theme(text = element_text(family = "Times"), 
        axis.text.x = element_text(angle = 90, hjust = 0.5, vjust = 0), 
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
  geom_text(aes(label = number_of_products), position = position_dodge(width = 0.9), vjust = -0.2, size = 3.5, family = "Times") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1, family = "Times")) + 
  theme(text = element_text(family = "Times")) +
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
  geom_text(aes(label = total_quantity_sold), position = position_dodge(width = 0.9), hjust = -0.2, size = 3.5, family = "Times") +
  coord_flip() +
  theme(text = element_text(family = "Times"), 
        axis.text.x = element_text(angle = 90, hjust = 1),
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
  geom_text(aes(label = sprintf("%.2f", average_discount)), position = position_dodge(width = 0.9), vjust = -0.2, size = 3.5, family = "Times", color = "black") +
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
  geom_text(aes(label = n), position = position_dodge(width = 0.9), vjust = -0.2, size = 3.5, family = "Times") +
  theme(text = element_text(family = "Times"), 
        axis.title = element_text(size = 12, family = "Times")) +
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
  geom_text(aes(label = sprintf("%.2f", total_revenue)), position = position_dodge(width = 0.9), vjust = -0.5, size = 3.5, family = "Times") +
  theme(text = element_text(family = "Times"), 
        axis.text.x = element_text(angle = 45, hjust = 1, family = "Times")) + 
  labs(title = "Monthly Revenue", x = "Month", y = "Revenue")
print(g_monthlyrevenue)

# Dynamically generate filename with current date and time
filename <- paste0("monthly_revenue_", 
                   format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")

# Save the plot with the dynamic filename
ggsave(filename, plot = g_monthlyrevenue, width = width, height = height)

RSQLite::dbDisconnect(connection)