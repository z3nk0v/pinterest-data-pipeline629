import requests
from time import sleep
import datetime
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            

            invoke_url_pin = 'https://k7b1w6u265.execute-api.us-east-1.amazonaws.com/stream/streaming-0ea7b76ff169-pin/record'
            payload_pin = json.dumps({
                'StreamName':'streaming-0ea7b76ff169-pin'
                'Data':
                    {
                    "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], 
                    "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], 
                    "save_location": pin_result["save_location"], "category": pin_result["category"]}
                    },
                    "PartitionKey": "desired-name"
                    })

            invoke_url_geo = 'https://k7b1w6u265.execute-api.us-east-1.amazonaws.com/stream/streaming-0ea7b76ff169.geo/record'
            payload_geo = json.dumps({
                'StreamName':'streaming-0ea7b76ff169-geo'
                'Data':
                    {
                    "value": {"ind": geo_result["ind"], "timestamp": geo_result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"), "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]}
                    }
                    "PartitionKey": "desired-name"
                    })

            invoke_url_user = 'https://k7b1w6u265.execute-api.us-east-1.amazonaws.com/stream/streaming-0ea7b76ff169.user'
            payload_user =json.dumps({
                'StreamName':'streaming-0ea7b76ff169-user'
                'Data': {
                    "ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"].strftime("%Y-%m-%d %H:%M:%S")
                        },
                    "PartitionKey": "desired-name"
                    })


            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            
            response_pin = requests.request("POST", invoke_url_pin, headers=headers, data=payload_pin)
            response_geo = requests.request("POST", invoke_url_geo, headers=headers, data=payload_geo)
            response_user = requests.request("POST", invoke_url_user, headers=headers, data=payload_user)
            
            print(response_pin.status_code)
            print(response_geo.status_code)
            print(response_user.status_code)
    
            

if __name__ == "__main__":
     run_infinite_post_data_loop()
     print('Working')
    