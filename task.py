import pandas as pd # type: ignore
import pickle
import re

class CustomerDataExtractor:
    def __init__(self, customer_file_path, vip_file_path):
        self.customer_file_path = customer_file_path
        self.vip_file_path = vip_file_path
        self.vip_ids = self._load_vip_ids()
        self.category_map = {
            1: 'Electronics',
            2: 'Apparel',
            3: 'Books',
            4: 'Home Goods'
        }

    def _load_vip_ids(self):
        with open(self.vip_file_path, 'r') as f:
            return set(int(line.strip()) for line in f if line.strip().isdigit())

    def _safe_parse_date(self, date_str):
        try:
            return pd.to_datetime(date_str, errors='raise')
        except Exception:
            try:
                parts = re.findall(r'\d+', str(date_str))
                if len(parts) >= 3:
                    year = int(parts[0])
                    month = min(max(1, int(parts[1])), 12)
                    day = min(max(1, int(parts[2])), 28)
                    return pd.Timestamp(year, month, day)
            except:
                return None
        return None

    def _extract_order_id(self, raw_order_id):
        match = re.search(r'\d+', str(raw_order_id))
        return int(match.group()) if match else None

    def load_and_transform(self):
        with open(self.customer_file_path, "rb") as f:
            customer_data = pickle.load(f)

        rows = []

        for customer in customer_data:
            customer_id = int(customer['id'])
            customer_name = str(customer.get('name', 'Unknown'))
            registration_date = self._safe_parse_date(customer.get('registration_date'))
            is_vip = customer_id in self.vip_ids

            for order in customer.get('orders', []):
                raw_order_id = order.get('order_id')
                order_id = self._extract_order_id(raw_order_id)
                order_date = self._safe_parse_date(order.get('order_date'))
                items = order.get('items', [])

                total_order_value = 0.0
                valid_items = []
                for item in items:
                    try:
                        price = float(item['price'])
                        quantity = int(item['quantity'])
                        valid_items.append((item, price, quantity))
                        total_order_value += price * quantity
                    except:
                        continue

                if not valid_items:
                    rows.append({
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "registration_date": registration_date,
                        "is_vip": is_vip,
                        "order_id": order_id,
                        "order_date": order_date,
                        "product_id": pd.NA,
                        "product_name": pd.NA,
                        "category": pd.NA,
                        "unit_price": pd.NA,
                        "item_quantity": pd.NA,
                        "total_item_price": pd.NA,
                        "total_order_value_percentage": pd.NA,
                    })
                    continue

                for item, price, quantity in valid_items:
                    product_id = item.get('item_id')
                    product_name = item.get('product_name')
                    category = self.category_map.get(item.get('category'), 'Misc')
                    total_item_price = price * quantity
                    total_order_value_percentage = (total_item_price / total_order_value) * 100 if total_order_value > 0 else 0

                    rows.append({
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "registration_date": registration_date,
                        "is_vip": is_vip,
                        "order_id": order_id,
                        "order_date": order_date,
                        "product_id": product_id,
                        "product_name": product_name,
                        "category": category,
                        "unit_price": price,
                        "item_quantity": quantity,
                        "total_item_price": total_item_price,
                        "total_order_value_percentage": total_order_value_percentage,
                    })

        df = pd.DataFrame(rows)

        # Remove rows where all item-level fields are missing
        item_fields = [
            "product_id", "product_name", "category",
            "unit_price", "item_quantity", "total_item_price", "total_order_value_percentage"
        ]
        df = df[~df[item_fields].isnull().all(axis=1)]

        # Round order_date to seconds to standardize
        df["order_date"] = pd.to_datetime(df["order_date"]).dt.round("S")

        # Fill missing order_id using (customer_id, order_date) lookup
        known_order_ids = (
            df.dropna(subset=["order_id"])[["customer_id", "order_date", "order_id"]]
            .drop_duplicates()
            .set_index(["customer_id", "order_date"])["order_id"]
        ).to_dict()

        def fill_missing_order_id(row):
            if pd.isna(row['order_id']):
                return known_order_ids.get((row['customer_id'], row['order_date']), pd.NA)
            return row['order_id']

        df["order_id"] = df.apply(fill_missing_order_id, axis=1)

        # Fill product_id and order_id from product_name if possible
        def extract_id_from_name(row, kind):
            if pd.notna(row['product_name']) and "Item" in row['product_name'] and "Order" in row['product_name']:
                match = re.search(r'Item (\d+) for Order (\d+)', row['product_name'])
                if match:
                    return int(match.group(1)) if kind == 'product_id' else int(match.group(2))
            return row[kind]

        df["product_id"] = df.apply(lambda r: extract_id_from_name(r, 'product_id'), axis=1)
        df["order_id"] = df.apply(lambda r: extract_id_from_name(r, 'order_id'), axis=1)

        # Fill missing product_name using order_id and product_id
        def fill_product_name(row):
            if pd.isna(row['product_name']) and pd.notna(row['order_id']) and pd.notna(row['product_id']):
                return f"Item {int(row['product_id'])} for Order {int(row['order_id'])}"
            return row['product_name']

        df["product_name"] = df.apply(fill_product_name, axis=1)

        # Enforce correct types
        dtype_map = {
            "customer_id": "Int64",
            "customer_name": "string",
            "registration_date": "datetime64[ns]",
            "is_vip": "boolean",
            "order_id": "Int64",
            "order_date": "datetime64[ns]",
            "product_id": "Int64",
            "product_name": "string",
            "category": "string",
            "unit_price": "float64",
            "item_quantity": "Int64",
            "total_item_price": "float64",
            "total_order_value_percentage": "float64",
        }

        df = df.astype(dtype_map)
        df = df.sort_values(by=["customer_id", "order_id", "product_id"])

        return df

extractor = CustomerDataExtractor("customer_orders.pkl", "vip_customers.txt")
df = extractor.load_and_transform()
df.to_csv("final_cleaned_customer_orders.csv", index=False)
