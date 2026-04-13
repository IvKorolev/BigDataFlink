import csv
import json
import time
import glob
from datetime import datetime
from kafka import KafkaProducer

def parse_date(date_str):
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        date_id = int(dt.strftime('%Y%m%d'))
        return {
            'date_id': date_id,
            'sql_date': dt.strftime('%Y-%m-%d'),
            'day': dt.day,
            'month': dt.month,
            'year': dt.year,
            'quarter': (dt.month - 1) // 3 + 1
        }
    except:
        return None

def main():
    print("Waiting for Kafka to start...")
    time.sleep(15)

    producer = KafkaProducer(
        bootstrap_servers=['kafka_lab3:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = 'shop_events'
    csv_files = glob.glob('/data/*.csv')

    stores, suppliers = {}, {}
    global_id_counter = 1

    for file in sorted(csv_files):
        print(f"Start reading file: {file}")
        with open(file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                row['id'] = global_id_counter
                global_id_counter += 1

                parsed_sale_date = parse_date(row['sale_date'])
                if parsed_sale_date:
                    row['date_id'] = parsed_sale_date['date_id']
                    row['sale_date_sql'] = parsed_sale_date['sql_date']
                    row['sale_day'] = parsed_sale_date['day']
                    row['sale_month'] = parsed_sale_date['month']
                    row['sale_year'] = parsed_sale_date['year']
                    row['sale_quarter'] = parsed_sale_date['quarter']

                prod_release = parse_date(row['product_release_date'])
                row['product_release_date_sql'] = prod_release['sql_date'] if prod_release else None
                prod_expiry = parse_date(row['product_expiry_date'])
                row['product_expiry_date_sql'] = prod_expiry['sql_date'] if prod_expiry else None

                store_email = row['store_email']
                if store_email not in stores: stores[store_email] = len(stores) + 1
                row['store_id'] = stores[store_email]

                supplier_email = row['supplier_email']
                if supplier_email not in suppliers: suppliers[supplier_email] = len(suppliers) + 1
                row['supplier_id'] = suppliers[supplier_email]

                producer.send(topic_name, value=row)
                time.sleep(0.005)

    producer.flush()
    print(f"Successfully sent {global_id_counter - 1} records to Kafka!")


if __name__ == '__main__':
    main()