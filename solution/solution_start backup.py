import argparse
import logging
import os
import pandas as pd
import glob

logging.basicConfig(filename='./logs/shopping_patterns.log')

class Extract:
    def extract_data(self, file_type: str = "", file_name=""):
        try:
            if file_type == "json":
                transaction_files = glob.glob("./input_data/starter/transactions/**/*.json", recursive=True)
                dfs = []
                for file in transaction_files:
                    try:
                        df = pd.read_json(file)
                        dfs.append(df)
                    except Exception as json_err:
                        print(f"Error reading JSON file {file}: {json_err}")
                if not dfs:
                    raise ValueError("No valid JSON files found.")
                df = pd.concat(dfs, ignore_index=True)
            elif file_type == "csv":
                df = pd.read_csv(f"./input_data/starter/{file_name}.csv")
            return df
        except Exception as err:
            logging.error(msg=str(err), exc_info=True)
            raise err

class Transform:
    def flatten(self, schema, prefix=None):
        fields = []
        for field in schema:
            name = prefix + '.' + field if prefix else field
            dtype = schema[field]
            if isinstance(dtype, dict):
                fields += self.flatten(dtype, prefix=name)
            else:
                fields.append(name)
        return fields

    def explodeDF(self, df):
        for col in df.columns:
            if isinstance(df[col].iloc[0], list):
                df = df.explode(col)
        return df

    def df_is_flat(self, df):
        for col in df.columns:
            if isinstance(df[col].iloc[0], list) or isinstance(df[col].iloc[0], dict):
                return False
        return True

    def flatJson(self, jdf):
        keepGoing = True
        while keepGoing:
            fields = self.flatten(jdf.dtypes.to_dict())
            new_fields = [item.replace(".", "_") for item in fields]
            jdf = jdf[new_fields]
            jdf = self.explodeDF(jdf)
            if self.df_is_flat(jdf):
                keepGoing = False
        return jdf

class Load:
    def load_data(self, file_type=""):
        global df
        if file_type == "json":
            transaction_files = glob.glob("transactions/*.json")
            dfs = [pd.read_json(file) for file in transaction_files]
            df = pd.concat(dfs, ignore_index=True)
        return df

    def to_landing(self, df):
        try:
            for customer_id, group_df in df.groupby("customer_id"):
                output_dir = f"output/customer_id={customer_id}"
                os.makedirs(output_dir, exist_ok=True)
                group_df.to_json(os.path.join(output_dir, "data.json"), orient="records", lines=True)
            logging.info("Files saved to landing zone.")
        except Exception as err:
            logging.error(msg=str(err), exc_info=True)
            raise err


class ShoppingPatternApp:
    def __init__(self, params) -> None:
        try:
            self.extract = Extract()
            self.transform = Transform()
            self.load = Load()
        except Exception as err:
            logging.error(msg=str(err), exc_info=True)
            raise err

    def run(self):
        try:
            extract = self.extract
            transform = self.transform
            load = self.load

            shoping = transform.flatJson(extract.extract_data(file_type="json"))
            customers_df = extract.extract_data(file_type="csv", file_name="customers")
            products_df = extract.extract_data(file_type="csv", file_name="products")

            ans = pd.merge(
                pd.merge(shoping, customers_df, left_on='customer_id', right_on='customer_id', how='inner'),
                products_df, left_on='basket_product_id', right_on='product_id', how='inner'
            )

            ans_grouped = ans.groupby(
                ['customer_id', 'loyalty_score', 'product_id', 'product_category']).size().reset_index(
                name='purchase_count')

            load.to_landing(df=ans_grouped)

        except Exception as err:
            logging.error(msg=str(err), exc_info=True)
            raise err


def main():
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    params = vars(parser.parse_args())

    shop_obj = ShoppingPatternApp(params)
    shop_obj.run()


if __name__ == "__main__":
    main()
