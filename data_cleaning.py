class DataCleaning:
    def __init__(self):
        pass

    def convert_product_weights(self, products_df):
        def convert_to_kg(weight):
            value, unit = weight.split()
            value = float(value)
            if unit in ['g', 'ml']:
                return value / 1000
            elif unit == 'kg':
                return value
            else:
                raise ValueError(f"Unknown unit: {unit}")

        products_df['weight'] = products_df['weight'].apply(convert_to_kg)
        return products_df

    def clean_products_data(self, products_df):
        clean_data = products_df.dropna()
        return clean_data

    def clean_store_data(self, data):
        clean_data = data.dropna()
        return clean_data

    def clean_orders_data(self, orders_df):
        columns to remove = ['first_name', 'last_name', '1']
        orders_df.drop(columns=columns to remove, inplace=True, errors='ignore')
        return orders_df
