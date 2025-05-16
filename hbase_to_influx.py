import happybase
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import sys
import requests
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hbase_to_influx.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def verify_influx_connection(url, max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{url}/ping")
            if response.status_code == 204:
                logging.info("InfluxDB is reachable")
                return True
            else:
                logging.warning(f"InfluxDB returned status code: {response.status_code}")
        except requests.exceptions.ConnectionError:
            logging.warning(f"Cannot connect to InfluxDB, attempt {attempt+1}/{max_retries}")
        
        if attempt < max_retries - 1:
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    return False

def write_points_with_retry(write_api, bucket, org, points, retries=3, delay=5):
    for attempt in range(retries):
        try:
            write_api.write(bucket=bucket, org=org, record=points)
            return True
        except Exception as e:
            logging.error(f"Write attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    return False

successful_writes = 0
batch_size = 500

try:

    logging.info("Connecting to HBase...")
    hbase = happybase.Connection('hadoop-master')
    table = hbase.table('air_quality')
    
    influx_url = "http://influxdb:8086"
    logging.info(f"Verifying connection to InfluxDB at {influx_url}...")
    
    if not verify_influx_connection(influx_url):
        logging.error("Failed to connect to InfluxDB after multiple attempts")
        logging.info("Please check if InfluxDB is running and accessible")
        sys.exit(1)
    
    client = InfluxDBClient(
        url=influx_url,
        token="token",
        org="rt4",
        timeout=60000 
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    logging.info("Starting data migration from HBase to InfluxDB...")
    points_batch = []
    
    for key, data in table.scan():
        try:
            station = data[b'geo:station_name'].decode()
            date = data[b'datetime:date'].decode()
            
            pm25 = float(data.get(b'pollution:PM2.5', b'0'))
            pm10 = float(data.get(b'pollution:PM10', b'0'))
            
            point = Point("pollution") \
                .tag("station", station) \
                .field("PM2.5", pm25) \
                .field("PM10", pm10) \
                .time(date)
            
            points_batch.append(point)
            
            if len(points_batch) >= batch_size:
                if write_points_with_retry(write_api, "air_quality", "wassim", points_batch):
                    successful_writes += len(points_batch)
                    logging.info(f"Successfully processed {successful_writes} records")
                    points_batch = []
                else:
                    logging.error(f"Failed to write batch ending at record {key}, skipping...")
                    points_batch = []
                    
        except Exception as e:
            logging.error(f"Error processing record {key}: {str(e)}")
    
    if points_batch:
        if write_points_with_retry(write_api, "air_quality", "wassim", points_batch):
            successful_writes += len(points_batch)
    
    write_api.close()
    client.close()
    hbase.close()
    
    logging.info(f"Migration complete. Successfully wrote {successful_writes} records to InfluxDB.")
    
except Exception as e:
    logging.error(f"Critical error: {str(e)}")
    sys.exit(1)
