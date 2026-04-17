import csv
import json
import time
import glob
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

def parse_date(date_str):
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return {
            'date_id': int(dt.strftime('%Y%m%d')),
            'sql_date': dt.strftime('%Y-%m-%d'),
            'day': dt.day,
            'month': dt.month,
            'year': dt.year,
            'quarter': (dt.month - 1) // 3 + 1
        }
    except:
        return None

def main():
    print("Waiting for Kafka and Topic to be ready...")
    time.sleep(15)

    scenario = os.environ.get('SCENARIO', 'A').upper()
    print(f"ЗАПУСК СЦЕНАРИЯ: {scenario}")

    producer = KafkaProducer(
        bootstrap_servers=['kafka_lab3:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = 'mock_data'
    csv_files = glob.glob('/data/*.csv')

    stores, suppliers, customers, sellers, products = {}, {}, {}, {}, {}
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

                if row['customer_email'] not in customers: customers[row['customer_email']] = len(customers) + 1
                row['sale_customer_id'] = customers[row['customer_email']]

                if row['seller_email'] not in sellers: sellers[row['seller_email']] = len(sellers) + 1
                row['sale_seller_id'] = sellers[row['seller_email']]

                if row['product_name'] not in products: products[row['product_name']] = len(products) + 1
                row['sale_product_id'] = products[row['product_name']]

                if row['store_email'] not in stores: stores[row['store_email']] = len(stores) + 1
                row['store_id'] = stores[row['store_email']]

                supplier_email = row['supplier_email']
                if supplier_email not in suppliers: suppliers[supplier_email] = len(suppliers) + 1
                row['supplier_id'] = suppliers[supplier_email]

                if scenario == 'A':
                    producer.send(topic_name, value=row)
                else:
                    # В качестве ключа возьмем ID магазина (все продажи одного магазина летят в одну партицию)
                    key_bytes = str(row['store_id']).encode('utf-8')
                    producer.send(topic_name, key=key_bytes, value=row)

                time.sleep(0.005)

    producer.flush()
    print(f"Успешно отправлено {global_id_counter - 1} сообщений в Kafka!\n")

    print("АНАЛИЗ НАГРУЗКИ ПАРТИЦИЙ")
    consumer = KafkaConsumer(bootstrap_servers=['kafka_lab3:9092'])
    partitions = consumer.partitions_for_topic(topic_name)

    if partitions:
        for p in sorted(partitions):
            tp = TopicPartition(topic_name, p)
            consumer.assign([tp])
            consumer.seek_to_end(tp)
            end_offset = consumer.position(tp)
            print(f"Партиция {p}: {end_offset} сообщений")
    consumer.close()


if __name__ == '__main__':
    main()