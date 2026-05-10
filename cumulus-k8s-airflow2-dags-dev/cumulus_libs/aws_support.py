# Databricks notebook source
from botocore.exceptions import ClientError
import boto3, json, os, math, requests, datetime, smtplib
from datetime import date, timedelta
from time import localtime, strftime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
# COMMAND ----------

# added for AWS secrets manager
def aws_secrets_manager_get_secret(secret_name, secrets_name_key, region_name="us-west-2"):
    # this needs to be v1.7.48 or greater    
    boto3v=boto3.__version__
    logging.debug("boto3 version: "+str(boto3v))

    client_aws_secret_mgr= boto3.client(
        service_name='secretsmanager',
        region_name=region_name)

    try:
        get_secret_value_response = client_aws_secret_mgr.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        else:
            print(str(e.response))
    else:
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
            secret_dict = json.loads(text_secret_data)
            return secret_dict[secrets_name_key]  # will let it fail if the key doesn't exist
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']
            return binary_secret_data


# COMMAND ----------

#returns float of current cost
#https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-pelong.html
#Service Endpoint
# AWS Price List Service API provides the following two endpoints:
# https://api.pricing.us-east-1.amazonaws.com
# https://api.pricing.ap-south-1.amazonaws.com
def get_on_demand_price(instance_type="", end_point_region_name="us-east-1", full_region_name="US West (Oregon)", os_product_desc="Linux/UNIX",service_code='AmazonEC2'):
    cost_str=None
    os=None
    # valid operating systems for api call
    OS_LIST = ['Windows', 'Linux', 'SUSE']
    for each_os in OS_LIST:
        if each_os in os_product_desc:
            os = each_os
    if not os:
        raise ValueError("Invalid Product Description %s"%(os_product_desc))

    # Search product filter
    FLT = """
        [{{"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"}},
        {{"Field": "operatingSystem", "Value": "{o}", "Type": "TERM_MATCH"}},
        {{"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"}},
        {{"Field": "instanceType", "Value": "{t}", "Type": "TERM_MATCH"}},
        {{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]
    """
    client = boto3.client('pricing', region_name=end_point_region_name) #must be either us-east-1 or ap-south-1
    f = FLT.format(r=full_region_name, t=instance_type, o=os)
    data = client.get_products(ServiceCode=service_code, Filters=json.loads(f))
    
    for each in data["PriceList"]:
        price_list = json.loads(each)
        if price_list['product']['attributes']['capacitystatus'] == "Used":
            price_list_on_demand_json= price_list['terms']['OnDemand']
            # get list of keys in above JSON
            keys_on_demand_json_list = list(price_list_on_demand_json.keys())
            price_list_price_dimensions_json = price_list_on_demand_json[keys_on_demand_json_list[0]]['priceDimensions']
            # get list of keys in above JSON
            keys_price_dimension_json_list = list(price_list_price_dimensions_json.keys())
            cost_json = price_list_price_dimensions_json[keys_price_dimension_json_list[0]]
            cost_str = cost_json['pricePerUnit']['USD']
            break
    
    if not cost_str:
      raise ValueError("Cost not found in Price list")
    print("on-demand cost: " + str(cost_str))
    return float(cost_str)

# COMMAND ----------

# this code returns a spot price to use for an instance in databricks. 
# it is based on the current best (lowest max price per AvailabilityZone) spot price in AWS for the last
# hour.  this spot price is divided by the current AWS on-demand price and multiplied by 100 to get the percentage. .5 percent is added
# to this number and rounded up to the next whole number.

# the function only needs the instance type.
# check_days define the number of days back from today that the function will check for the highest spot price.  default is 1
# region_name is the region to check for the spot price. default is us-west-2
# ProductDescription is the Operating systems.  defaults to "Linux/UNIX"
def get_spot_pricing(instance_type="", check_days=1, region_name="us-west-2", full_region_name="US West (Oregon)",os_product_desc="Linux/UNIX",client_type="ec2"):
    print("""
        Checking with parameters:
        instance_type: %s,
        check_days=%s,
        region_name=%s, 
        os_product_desc=%s,
        client_type=%s    
    """%(str(instance_type),str(check_days),str(region_name),str(os_product_desc),str(client_type)))

    # check_days is positive integer to check how many days in the past to verify spot price
    # full_region_name and ProductionDescription choices are noted at start of notebook
    # created formated date to match 'TimeStamp' in spot price history
    NOW = datetime.datetime.utcnow()
    minus_dif = datetime.timedelta(days=(check_days*-1))
    now_minus_dif = NOW + minus_dif
    timestamp_price_history_check = "{:%Y-%m-%d %H:%M:%S}".format(now_minus_dif)
    timestamp_price_history_check += "+00:00"

    print("----------------------------------------")
    print("    ... Checking history from '" + str(timestamp_price_history_check) + "' to now.")

    client = boto3.client(client_type, region_name=region_name)
    prices = client.describe_spot_price_history(InstanceTypes=[instance_type])

    spot_price_history = prices['SpotPriceHistory']
    zone_price_json = {"AvailabilityZonePrices": {}}
    for each in spot_price_history:
        if each["ProductDescription"] == os_product_desc:
            timestamp_str = "{:%Y-%m-%d %H:%M:%S}".format(each["Timestamp"])
            timestamp_str += "+00:00"

            if timestamp_str > timestamp_price_history_check:
                if each['AvailabilityZone'] in zone_price_json['AvailabilityZonePrices']:
                    zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['count'] += 1
                    zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['total-of-prices'] += float(each['SpotPrice'])

                    if each['SpotPrice'] < zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['min-price']:
                        zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['min-price'] = each['SpotPrice']
                        zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['min-price-timestamp'] = str(each['Timestamp'])
                    if each['SpotPrice'] > zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['max-price']:
                        zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['max-price'] = each['SpotPrice']
                        zone_price_json['AvailabilityZonePrices'][each['AvailabilityZone']]['max-price-timestamp'] = str(each['Timestamp'])
                else:
                    zone_pricing_final = {each['AvailabilityZone']: {
                        "min-price": each['SpotPrice'],
                        "max-price": each['SpotPrice'],
                        "min-price-timestamp": str(each['Timestamp']),
                        "max-price-timestamp": str(each['Timestamp']),
                        "total-of-prices": float(each['SpotPrice']),
                        "count": 1
                    }}
                    zone_price_json["AvailabilityZonePrices"].update(zone_pricing_final)

    availability_zone_prices = zone_price_json["AvailabilityZonePrices"]
    best_max_price = 100000.0
    best_min_price = 100000.0
    for key, value in availability_zone_prices.items():
        line_summary = "%s %s %s %s" % (
            key, value['max-price'], value['min-price'], str(value['total-of-prices']/value['count']))
        print(line_summary)

        if float(value['max-price']) < best_max_price:
            availability_zone_max = key
            best_max_price = float(value['max-price'])
            best_max_price_str = value['max-price']

        if float(value['min-price']) < best_min_price:
            availability_zone_min = key
            best_min_price = float(value['min-price'])
            best_min_price_str = value['min-price']
    #us-east-1 is our only valid endpoint let default take care of it
    on_demand_cost = get_on_demand_price(instance_type=instance_type, end_point_region_name="us-east-1", full_region_name=full_region_name, os_product_desc=os_product_desc)    
    percentage = float(best_max_price_str)/on_demand_cost*100
    new_percent_whole_number = int(math.ceil(percentage + .5))

    print("AvailabilityZonePrices Summary")
    print("AvailZone  MaxPrice MinPrice AvgPrice")
    print("-----------------------------------------")

    print("Best AvailabilityZone - Price")
    print("     AvailZone  Price")
    print("-------------------------")
    print("max: %s %s" % (availability_zone_max, best_max_price_str))
    print("min: %s %s" % (availability_zone_min, best_min_price_str))
    print("On Demand Cost: "+str(on_demand_cost))
    print("actual percentage: " + str(percentage))
    print("returning suggested percentage for spot price: " +str(new_percent_whole_number))
    print("returning best AvailabilityZone: " + availability_zone_max)
    return new_percent_whole_number, availability_zone_max

# COMMAND ----------
