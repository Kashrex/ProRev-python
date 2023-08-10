import argparse
import os
import pandas as pd
from datetime import datetime, timedelta


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def load_and_preprocess_customers(file_path):
    customers_df = pd.read_csv(file_path)
    # Additional preprocessing if needed
    return customers_df


def load_and_preprocess_products(file_path):
    products_df = pd.read_csv(file_path)
    # Additional preprocessing if needed
    return products_df


def load_and_preprocess_transactions(file_path):
    transactions_df = pd.read_json(file_path, lines=True)
    # Additional preprocessing if needed
    return transactions_df


def analyze_shopping_patterns(customers_df, transactions_df, products_df):
    # Implement your analysis logic here
    # For example, calculate total spending, most purchased products, etc.
    analyzed_results = {}  # Placeholder for analysis results
    return analyzed_results


def main():
    params = get_params()

    # Load and preprocess data
    customers_data = load_and_preprocess_customers(params['customers_location'])
    products_data = load_and_preprocess_products(params['products_location'])

    # Initialize dictionary to store analyzed results
    analyzed_results = {}

    # Loop through transactions files and analyze data
    transaction_files = os.listdir(params['transactions_location'])
    for file_name in transaction_files:
        if file_name.endswith(".json"):
            transaction_data = load_and_preprocess_transactions(
                os.path.join(params['transactions_location'], file_name))
            analysis = analyze_shopping_patterns(customers_data, transaction_data, products_data)
            analyzed_results[file_name] = analysis

    # Save analyzed results for downstream use
    output_directory = params['output_location']
    os.makedirs(output_directory, exist_ok=True)
    for file_name, analysis in analyzed_results.items():
        output_path = os.path.join(output_directory, file_name.replace(".json", "_analysis.json"))
        with open(output_path, 'w') as f:
            # You can customize the format of the saved analysis results
            f.write(json.dumps(analysis))


if __name__ == "__main__":
    main()
