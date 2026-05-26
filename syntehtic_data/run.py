import os
import importlib


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(os.path.join(script_dir, "data"), exist_ok=True)

    sql_db = importlib.import_module("sql_db")
    sql_db.generate_data_for_sql_db()

    historical_orders = importlib.import_module("historical_orders")
    historical_orders.generate_historical_orders()

    reviews = importlib.import_module("reviews")
    reviews.generate_customer_reviews(review_percentage=0.01)
