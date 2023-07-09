import calendar
from datetime import datetime, timedelta, date
import dateutil.relativedelta as rdelta
from dateutil.relativedelta import relativedelta, SU
import requests
import time
import json
from decimal import Decimal

import os
import logging
from sqlite3 import Date
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

LOGGING_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO, datefmt=DATE_FORMAT)

# Create a connection to local db
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
                          region_name='us-east-1',
                          aws_access_key_id='key',
                          aws_secret_access_key='dummy')
# Create a connection to dynamodb
ddb = boto3.resource('dynamodb',
                     endpoint_url='https://dynamodb.eu-west-1.amazonaws.com',
                     region_name='eu-west-1',
                     aws_access_key_id='AKIAQXOOUSBLQ6CV3NDG',
                     aws_secret_access_key='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'
                     )

TABLE_NAME = 'opsuitestaging'
table = ddb.Table(TABLE_NAME)

def query_dynamodb(table, pk, sk1=None, sk2=None):
    transactions = []

    try:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(pk).__and__(
                Key('SK').between(sk1, sk2)),
            ProjectionExpression='SK, Thing_Organization_LocalBusiness_locationId, Thing_Intangible_Offer_subtotal')
        transactions.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('PK').eq(pk).__and__(
                    Key('SK').between(sk1, sk2)),
                ExclusiveStartKey=response['LastEvaluatedKey'],
                ProjectionExpression='SK, Thing_Organization_LocalBusiness_locationId, Thing_Intangible_Offer_subtotal')
            transactions.extend(response['Items'])
        return transactions
    except ClientError as err:

        raise

def query_pot(table, pk, loc_id):
    pot = []
    ean = {
        "#dep": "Thing_Property-department_Organization_identifier"
    }
    try:
        response = table.query(
            ExpressionAttributeNames=ean,
            KeyConditionExpression=Key('PK').eq(pk),
            ProjectionExpression='Thing_Intangible_Order_profit, '
                                 'Thing_Intangible_Order_profitMargin, '
                                 'Thing_Intangible_OrderItem_orderQuantity, '
                                 'Thing_Intangible_StructuredValue_PriceSpecification_salesTax,'
                                 '#dep,'
                                 'Thing_Intangible_DefinedTerm_CategoryCode_identifier,'
                                 'Thing_Organization_identifier')

        for i in response['Items']:
            i['loc_id'] = loc_id
        return response['Items']
    except ClientError as err:
        # logger.error(
        #     "Couldn't query for movies released in %s. Here's why: %s: %s", year,
        #     err.response['Error']['Code'], err.response['Error']['Message'])
        raise err


def lambda_handler(event, context):

    event = event["body"]
    event = json.loads(event)
    transaction_count = 0
    start_date = datetime.strptime(event['start_date'], TIME_FORMAT)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(hours=2)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    next_time = start_date
    # location = event['location_id']
    # location=19
    partition_key = f'TRA#{start_date.strftime("%Y-%m")}'
    sort_key_start = f'TRA#{start_date.strftime("%Y-%m-%d %H:%M:%S")}'
    sort_key_end = f'TRA#{start_date.replace(hour=23, minute=59, second=59, microsecond=0)}'

    logging.info("Intention to use web DynamoDB resource confirmed")
    table = ddb.Table('opsuitestaging')
    TABLE = dynamodb.Table('AggDataHourly')
    transactions = query_dynamodb(table, partition_key, sort_key_start, sort_key_end)

    # Split data based on one-hour intervals
    hourly_data = {}
    for trans in transactions:
        sk = trans['SK']
        hour = sk.split('#')[1][:13]  # Extract the hour from the sort key

        if hour not in hourly_data:
            hourly_data[hour] = []

        hourly_data[hour].append(trans)

    # Print the hourly data
    for hour, data in hourly_data.items():
        print(f"Hour: {hour}")
        try:
            aggregated_data = {}
            transaction_count_data = {}

            for trans in data:
                transactions = []
                product_on_transactions = []
                # print(trans)
                transaction_pk = f"TRA#{trans['SK'].split('#')[-1]}"
                pot = query_pot(table, transaction_pk,
                                trans['Thing_Organization_LocalBusiness_locationId'])
                # print(pot);
                if trans['Thing_Organization_LocalBusiness_locationId'] in transaction_count_data:
                    transaction_count_data[trans['Thing_Organization_LocalBusiness_locationId']][
                        'tra'] += 1
                    transaction_count_data[trans['Thing_Organization_LocalBusiness_locationId']][
                        'total_sales'] += \
                        trans['Thing_Intangible_Offer_subtotal']
                    transaction_count_data[trans['Thing_Organization_LocalBusiness_locationId']][
                        'pot'] += len(pot)
                else:
                    transaction_count_data.update(
                        {
                            trans['Thing_Organization_LocalBusiness_locationId']: {
                                'tra': 1,
                                'pot': len(pot),
                                'total_sales': trans['Thing_Intangible_Offer_subtotal']
                            }
                        }
                    )
                if pot:
                    product_on_transactions.extend(pot)
                    transaction_count += 1
        except Exception as e:
            raise e
        else:
            logging.info("data fetched")
            total_transactions = len(transactions)
            total_items = len(product_on_transactions)
            logging.info(f"pot size {(total_items)}#{total_transactions}")
            if transaction_count != 0:
                items_per_transaction = total_items / transaction_count
            # dept_wise_count = dict(
            #     Counter(p['Thing_Property-department_Organization_identifier'] for p in product_on_transactions))
            total_profit = {}
            for product in product_on_transactions:
                dept_id = product['Thing_Property-department_Organization_identifier']
                # category_id = product['Thing_Intangible_DefinedTerm_CategoryCode_identifier']
                loc_id = product['loc_id']
                if loc_id in total_profit:
                    total_profit[loc_id].update(
                        {
                            dept_id: {

                                'total_profit': float(product.get('Thing_Intangible_Order_profit', 0)),
                                'total_profit_margin': float(
                                    product.get('Thing_Intangible_Order_profitMargin', 0)),
                                'total_sales_tax': float(
                                    product.get('Thing_Intangible_StructuredValue_PriceSpecification_salesTax',
                                                0)),
                                'total_quantity': float(
                                    product.get('Thing_Intangible_OrderItem_orderQuantity', 0)),
                                'total_items': 1

                            }
                        })
                else:
                    total_profit.update(
                        {
                            loc_id: {

                                'total_profit': float(product.get('Thing_Intangible_Order_profit', 0)),
                                'total_profit_margin': float(
                                    product.get('Thing_Intangible_Order_profitMargin', 0)),
                                'total_sales_tax': float(
                                    product.get('Thing_Intangible_StructuredValue_PriceSpecification_salesTax',
                                                0)),
                                'total_quantity': float(
                                    product.get('Thing_Intangible_OrderItem_orderQuantity', 0)),
                                'total_items': 1

                            }

                        })
                resp = []
                for location in total_profit:
                    resp.append(
                        {
                            'PK': f"{start_date.strftime('AGG#%Y')}",
                            # 'SK': f"{start_date.strftime('%Y-%m-%d %H:%M:%S')}-{next_time.strftime('%H:%M:%S')}#{str(location)}",
                            # 'SK': f"{hour}#{str(location)}",
                            # 'SK': f"{hour}#{str(location)}",
                            'SK': f"{hour} #{str(location)}",

                            'total_profit': str(round(total_profit[location]['total_profit'], 2)),
                            'total_profit_margin': str(round(total_profit[location]['total_profit_margin'], 2)),
                            'total_sales_tax': str(round(total_profit[location]['total_sales_tax'], 2)),
                            'total_quantity': int(total_profit[location]['total_quantity']),
                            'total_items': int(total_profit[location]['total_items']),
                            'loc_id': int(location),
                            # 'dept_id': int(dept),
                            # 'transaction_count': transaction_count_data[location]['tra'],
                            # 'product_count': transaction_count_data[location]['pot'],
                            # 'total_sales': transaction_count_data[location]['total_sales']
                        }
                    )
            resp_json = json.dumps(resp, cls=DecimalEncoder)

            resp_float = json.loads(resp_json, parse_float=Decimal)


            with TABLE.batch_writer() as batch:
                for item in resp_float:
                    try:
                        # print(item)
                        response = batch.put_item(item)
                    except:
                        # print(item)
                        raise

    # while next_time <= end_date:
    #     try:
    #
    #     except Exception as e:
    #
    #         raise e
    #     else:

    prev_time = next_time
    next_time += timedelta(hours=1)
    # print(next_time)

    if next_time.strftime('%H:%M:%S') == '00:00:00':
        next_time = end_date
    return "Success"


if __name__ == "__main__":
    event = {
        "body": "{\"start_date\": \"2021-12-01T00:00:00.000\"}"
    }
    context = None

    print(lambda_handler(event, context))