# ST_Task

# 🧾 Customer Order Extraction and Transformation

This project extracts, cleans, and transforms nested customer order data from a binary `.pkl` file into a structured, flat DataFrame ready for analysis.

## 📦 Files

- `task.py`: Python script containing the `CustomerDataExtractor` class
- `customer_orders.pkl`: Pickled dataset with nested customer order records
- `vip_customers.txt`: List of VIP customer IDs (one per line)
- `final_cleaned_customer_orders.csv`: Output file containing the transformed data
- `README.md`: Project documentation

## ✅ Features

The `CustomerDataExtractor` performs the following:

- 🔓 **Loads pickled binary data** (`customer_orders.pkl`)
- ⭐ **Marks VIP customers** using `vip_customers.txt`
- 📅 **Fixes and parses invalid dates** (e.g., converts `2023-13-01` to `2023-12-01`)
- 🔁 **Fills missing `order_id`s** using:
  - Matching `customer_id + exact order_date (to the second)`
- 🧩 **Fills missing `product_name`**, `product_id`, and `order_id` using the naming pattern:

- 🧹 **Removes rows with completely missing item-level fields**
- ✅ **Enforces proper data types** using pandas nullable types (`Int64`, `boolean`, etc.)
- 📊 **Sorts the final DataFrame** by `customer_id`, `order_id`, and `product_id`

## 📁 Input File Format

### `customer_orders.pkl`
A pickled list of customer dictionaries. Each customer includes:
- `id`: int
- `name`: str
- `registration_date`: str (may be invalid)
- `orders`: list of orders  
Each order includes:
- `order_id`: str (e.g., `"ORD5"`)
- `order_date`: str
- `items`: list of item dictionaries  
  Each item includes:
  - `item_id`: int
  - `product_name`: str
  - `category`: int (1–4, or unmapped)
  - `price`: float
  - `quantity`: int

### `vip_customers.txt`
Text file with one VIP customer ID per line.

## 🛠️ How to Run

1. Ensure `customer_orders.pkl` and `vip_customers.txt` are in the same directory as `task.py`.
2. Run the script:

```bash
python task.py
