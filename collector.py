import requests
import os
from datetime import datetime, timedelta
import json
from json import dumps
from kafka import KafkaProducer
import time


def log(message: str):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%dT%H:%M%S') + \
        ('.%03d' % (now.microsecond / 10000))
    
    print('[', timestamp, ']', message)

def readNumericalEnvVar(varName: str, default: int) -> int:
    if varName in os.environ and os.environ[varName].isnumeric:
        return int(os.environ[varName])
    else:
        return default
    
class GetNumbersData():
    
    TOKEN_URL = #Hiding due to sensitivity

    def execute(self, clientId: str, clientSecret: str, resource: str, kafka_bootstrap_servers: list, kafka_topic: str, sec_protocol: str, ssl_certpath: str, ssl_keypath: str, ssl_pass: str,ssl_cafile: str, data_in_page: str):
        
        access_token = self.get_access_token(clientId, clientSecret, resource)
        self.request_and_ship_list_numbers(access_token, kafka_bootstrap_servers,kafka_topic,sec_protocol,ssl_certpath,ssl_keypath,ssl_pass,ssl_cafile,data_in_page)

    def get_access_token(self, clientId: str, clientSecret: str, resource: str) -> str:
        payload = #Hiding due to sensitivity
        files = #Hiding due to sensitivity
        headers =  #Hiding due to sensitivity

        log("Requesting access token")
        response = requests.request("POST", self.TOKEN_URL, headers=headers, data=payload, files=files)
        log("Auth response: " + response.text)
        return response.json()['access_token']
    
    def request_and_ship_list_numbers(self, access_token: str, kafka_bootstrap_servers: list, kafka_topic: str, sec_protocol: str, ssl_certpath: str, ssl_keypath: str, ssl_pass: str, ssl_cafile: str,data_in_page: str):
        
        producer = KafkaProducer(bootstrap_servers = kafka_bootstrap_servers,
                                 value_serializer = lambda v:
                                 json.dumps(v).encode('utf-8'),
                                 security_protocol = sec_protocol,
                                 ssl_check_hostname=True,
                                 ssl_cafile= ssl_cafile,
                                 ssl_certfile = ssl_certpath,
                                 ssl_keyfile = ssl_keypath,
                                 ssl_password = ssl_pass
                                 )

        payload = ""
        headers = {#Hiding due to sensitivity
            }
        skip_file = 0
        numbers_counter = 0
        
        api_url = #Hiding due to sensitivity
        if "API_URL" in os.environ:
            api_url = os.environ["API_URL"]
            log("API Path provided, using: ",api_url)
        log("API Path not provided, using default API Path: "+ api_url)
        
        allData = False
        while allData == False:
            api_url = #Hiding due to sensitivity
            log("Requesting data using: " + api_url)
            response = requests.request("GET",api_url,headers=headers,data=payload)
            skip_file += int(data_in_page)
            data = response.json()
            if len(data["NumberDetails"]) > 0:
                for number in data["NumberDetails"]:
                    numbers_counter += 1
                    producer.send(kafka_topic, value= number)
            else:
                allData = True
        log("Total of "+ str(numbers_counter) +" shipped to kafka.")

def main():
    banner = """
######                                                                 
#     #  ####  #    # #    # #       ####    ##   #####  ###### #####  
#     # #    # #    # ##   # #      #    #  #  #  #    # #      #    # 
#     # #    # #    # # #  # #      #    # #    # #    # #####  #    # 
#     # #    # # ## # #  # # #      #    # ###### #    # #      #####  
#     # #    # ##  ## #   ## #      #    # #    # #    # #      #   #  
######   ####  #    # #    # ######  ####  #    # #####  ###### #    # 
"""
    print(banner)

    execMinutes = #Hiding due to sensitivity
    initialDelayMinutes = #Hiding due to sensitivity

    authKeys = ["CLIENT_ID", "CLIENT_SECRET", "RESOURCE", "DATA_IN_PAGE"]
    
    if all(key in os.environ for key in authKeys):
        clientId = os.environ['CLIENT_ID']
        clientSecret = os.environ['CLIENT_SECRET']
        resource = os.environ['RESOURCE']
        data_in_page = os.environ["DATA_IN_PAGE"]
    else:
        clientId,clientSecret,resource = "", "", ""
        log("Please provide the following ENV variables required for authentication: " + ", ".join(authKeys))
        return -1
    
    requiredKafka_config = ["KAFKA_BOOTSTRAP_SERVER", "KAFKA_TOPIC"]
    if all(configs in os.environ for configs in requiredKafka_config):
       kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVER"]
       kafka_topic = os.environ["KAFKA_TOPIC"]
    else:
       kafka_bootstrap_servers, kafka_topic = "", ""
       log("Please provide the following ENV variables required for Kafka producer:"+ ", ".join(requiredKafka_config))
       return -1
    kafka_bootstrap_servers = kafka_bootstrap_servers.replace(" ","").split(",")

    tls = ["SSL_CERTPATH","SSL_KEYPATH","SSL_PWD","SSL_CAFILE"]
    sec_protocol = os.environ["SECURITY_PROTOCOL"]
    if sec_protocol.upper() == "SSL":
        log("SSL will be used for security protocol")
        if all(certs in os.environ for certs in tls):
            ssl_certpath = os.environ["SSL_CERTPATH"]
            ssl_keypath = os.environ["SSL_KEYPATH"]
            ssl_pass = os.environ["SSL_PWD"]
            ssl_cafile = os.environ["SSL_CAFILE"]
        else:
            ssl_certpath,ssl_keypath,ssl_pass,ssl_cafile = "","","",""
            log("Please provide the following variables required to enable TLS:"+ ", ".join(tls))
            return -1
    else:
        sec_protocol = "PLAINTEXT"    
        ssl_certpath = None
        ssl_keypath = None
        ssl_pass = None
        ssl_cafile = None

    new = GetNumbersData()
    log("The first file download will be attempted in " + str(initialDelayMinutes) + " minute(s).")
    time.sleep(60 * initialDelayMinutes)
    while True:
        new.execute(clientId, clientSecret, resource, kafka_bootstrap_servers, kafka_topic,sec_protocol,ssl_certpath,ssl_keypath,ssl_pass,ssl_cafile,data_in_page)
        log("Next file download will be attempted in " + str(execMinutes) + " minute(s).")
        time.sleep(60 * execMinutes)

if __name__ == "__main__":
    main()
