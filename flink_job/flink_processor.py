from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.execute_sql("""
        CREATE TABLE kafka_source (
            id INT,

            sale_customer_id INT, customer_first_name STRING, customer_last_name STRING, 
            customer_age INT, customer_email STRING, customer_country STRING, 
            customer_postal_code STRING, customer_pet_type STRING, customer_pet_name STRING, 
            customer_pet_breed STRING, pet_category STRING,

            sale_seller_id INT, seller_first_name STRING, seller_last_name STRING, 
            seller_email STRING, seller_country STRING, seller_postal_code STRING,

            sale_product_id INT, product_name STRING, product_category STRING, 
            product_price DOUBLE, product_quantity INT, product_weight DOUBLE,
            product_color STRING, product_size STRING, product_brand STRING, 
            product_material STRING, product_description STRING,
            product_rating DOUBLE, product_reviews INT, 
            product_release_date_sql STRING, product_expiry_date_sql STRING,

            store_id INT, store_name STRING, store_location STRING, store_city STRING, 
            store_state STRING, store_country STRING, store_phone STRING, store_email STRING,

            supplier_id INT, supplier_name STRING, supplier_contact STRING, 
            supplier_email STRING, supplier_phone STRING, supplier_address STRING, 
            supplier_city STRING, supplier_country STRING,

            date_id INT, sale_date_sql STRING, sale_day INT, sale_month INT, 
            sale_year INT, sale_quarter INT,
            sale_quantity INT, sale_total_price DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mock_data',
            'properties.bootstrap.servers' = 'kafka_lab3:9092',
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    def create_pg_sink(table_name, schema):
        return f"""
            CREATE TABLE {table_name} (
                {schema}
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://postgres:5432/lab3_db',
                'table-name' = '{table_name}',
                'username' = 'postgres',
                'password' = 'postgres'
            )
        """

    table_env.execute_sql(create_pg_sink('dim_customer',
                                         'customer_id INT PRIMARY KEY NOT ENFORCED, first_name STRING, last_name STRING, age INT, email STRING, country STRING, postal_code STRING, pet_type STRING, pet_name STRING, pet_breed STRING, pet_category STRING'))
    table_env.execute_sql(create_pg_sink('dim_product',
                                         'product_id INT PRIMARY KEY NOT ENFORCED, product_name STRING, product_category STRING, product_price DOUBLE, product_quantity INT, product_weight DOUBLE, product_color STRING, product_size STRING, product_brand STRING, product_material STRING, product_description STRING, product_rating DOUBLE, product_reviews INT, product_release_date DATE, product_expiry_date DATE'))
    table_env.execute_sql(create_pg_sink('dim_seller',
                                         'seller_id INT PRIMARY KEY NOT ENFORCED, first_name STRING, last_name STRING, email STRING, country STRING, postal_code STRING'))
    table_env.execute_sql(create_pg_sink('dim_store',
                                         'store_id INT PRIMARY KEY NOT ENFORCED, store_name STRING, store_location STRING, store_city STRING, store_state STRING, store_country STRING, store_phone STRING, store_email STRING'))
    table_env.execute_sql(create_pg_sink('dim_supplier',
                                         'supplier_id INT PRIMARY KEY NOT ENFORCED, supplier_name STRING, supplier_contact STRING, supplier_email STRING, supplier_phone STRING, supplier_address STRING, supplier_city STRING, supplier_country STRING'))
    table_env.execute_sql(create_pg_sink('dim_date',
                                         'date_id INT PRIMARY KEY NOT ENFORCED, full_date DATE, `day` INT, `month` INT, `year` INT, `quarter` INT'))
    table_env.execute_sql(create_pg_sink('fact_sales',
                                         'sale_id INT PRIMARY KEY NOT ENFORCED, date_id INT, customer_id INT, seller_id INT, product_id INT, store_id INT, supplier_id INT, sale_quantity INT, sale_total_price DOUBLE'))

    stmt_set = table_env.create_statement_set()

    stmt_set.add_insert_sql(
        "INSERT INTO dim_customer SELECT sale_customer_id, customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed, pet_category FROM kafka_source")
    stmt_set.add_insert_sql(
        "INSERT INTO dim_product SELECT sale_product_id, product_name, product_category, product_price, product_quantity, product_weight, product_color, product_size, product_brand, product_material, product_description, product_rating, product_reviews, CAST(product_release_date_sql AS DATE), CAST(product_expiry_date_sql AS DATE) FROM kafka_source")
    stmt_set.add_insert_sql(
        "INSERT INTO dim_seller SELECT sale_seller_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code FROM kafka_source")
    stmt_set.add_insert_sql(
        "INSERT INTO dim_store SELECT store_id, store_name, store_location, store_city, store_state, store_country, store_phone, store_email FROM kafka_source")
    stmt_set.add_insert_sql(
        "INSERT INTO dim_supplier SELECT supplier_id, supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country FROM kafka_source")
    stmt_set.add_insert_sql(
        "INSERT INTO dim_date SELECT date_id, CAST(sale_date_sql AS DATE), sale_day, sale_month, sale_year, sale_quarter FROM kafka_source WHERE date_id IS NOT NULL")

    stmt_set.add_insert_sql(
        "INSERT INTO fact_sales SELECT id, date_id, sale_customer_id, sale_seller_id, sale_product_id, store_id, supplier_id, sale_quantity, sale_total_price FROM kafka_source")

    stmt_set.execute()

if __name__ == '__main__':
    main()