# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 10:05:37 2025
IOT sensors data collection using MQTT
@author: heusi001

(install or upgrade paho-mqtt, because it needs verion 2 or higher)
pip install --upgrade paho-mqtt
data stored in CSV_FILE
Automatic new column generation with each new sensor deployed
Datetime stored as UTC
Instrument type selection based on first characters of a device_id
Currently supported:
    temperature logger  (name starts with temperature)
    temperature and humidity logger (name starts with th)
    leaf wetness logger (name starts with lws)
    SDI-12logger for SD-2 or Atmos22 anemometers (name starts with u)
Filename: automatic dayfile generation with format IOT_data_yyyymmdd.csv
With the start of a new day it automatically loads the header of the previous day.
This is important so that the column order is kept the same.
(otherwise the first new column would be the first sensor that sends data)
"""

import paho.mqtt.client as mqtt
import re
import json

import base64
import struct
from datetime import datetime
import pandas as pd
import os

from time import gmtime, strftime, time
from datetime import datetime, timedelta, timezone
##mydate=strftime("%Y%m%d", gmtime())
#mydate='20250321' 

##CSV_FILE = "IOT_data_"+mydate+".csv"

APP_ID = 'aams-network'
DEVICE_ID = 'A8404180D65C1F8D' #temperature2_d20
USERNAME = f'{APP_ID}@ttn'
#PASSWORD = 'NNSXS.4D5PROE4BBT4PFWN7QK47D2B66PRUAKOBRY6PNY.FY5ROEKHFTZ625OWRAWFR3FJ7OWAZ3W5EBOMMO5LZNIM6M3GFD4Q'
PASSWORD = 'NNSXS.XMIVXHIFGML35KDSSH7TRJL5JO7IMF7M5UAEAFI.N3UDOBLEBI3BK2IQRQ665SZLSK3QB3JUY66WKLBBISAV4SGOK3SA' #new 20250417
BROKER = 'eu1.cloud.thethings.network'
PORT = 8883  # TLS

TOPIC = f'v3/{APP_ID}@ttn/devices/+/up'

def on_connect(client, userdata, flags, reasonCode, properties=None):
    print("Connected with reason code:", reasonCode)
    client.subscribe(TOPIC)
'''
def on_message(client, userdata, message):
    print(f"\nMessage received on topic: {message.topic}")
    payload = json.loads(message.payload.decode())
    print(json.dumps(payload, indent=2))
'''

def on_message(client, userdata, message):
    # Parse JSON payload
    data = json.loads(message.payload.decode())

    # Extract uplink_message
    uplink = data["uplink_message"]
    device_id = data["end_device_ids"]["device_id"]
    print("device_id:"+device_id)

    # --- Get timestamp from TTN or fallback to local time ---
    ttn_timestamp = uplink.get("received_at")
    timestamp = ttn_timestamp if ttn_timestamp else datetime.utcnow().isoformat()
    
    # Get and decode frm_payload (base64)
    b64_payload = uplink["frm_payload"]
    if not(b64_payload==None):  #sometimes the sensor data payload is empty
        decoded_bytes = base64.b64decode(b64_payload)
    
        # --- Device-specific decoding ---
        '''
        example for th sensor, which produces:
            "0F7868010163000081024E"
            it sends 'D3hoAQFjAACBAk4='
        this is decoded with decoded_bytes=base64.b64decode('D3hoAQFjAACBAk4=')
        decoded_bytes now contains: b'\x0fxh\x01\x01c\x00\x00\x81\x02N'
        
        battery_mv, unix_timestamp, alarm, temp_raw, humidity_raw = struct.unpack(">HIBhH", decoded_bytes[:11])
        gives the following output (battery mV, T and h times 10): 3960,129,590
        
        Anemometer (sends ascii data separated with +):
        data to TTN: "0D5001312B302E36332B33342E372B312E35300D0A000000"
        data send from TTN: "DVABMSswLjYzKzM0LjcrMS41MA0KAAAA"
            
        '''
        if device_id[:2] == "th":
            # Unpack data (battery: uint16, timestamp: uint32, alarm: uint8, temp: int16, humidity: uint16)
            battery_mv, unix_timestamp, alarm, temp_raw, humidity_raw = struct.unpack(">HIBhH", decoded_bytes[:11])
            
            # Convert temperature and humidity
            temp_c = temp_raw / 10
            humidity = humidity_raw / 10
    
            # Format the timestamp from Unix timestamp (if needed)
            # Optionally, you can use the TTN timestamp instead if it's more accurate
            #timestamp = datetime.utcfromtimestamp(unix_timestamp).isoformat()
    
            print(f"[{timestamp}] {device_id} | Battery: {battery_mv} mV | Temp: {temp_c:.1f} °C | Humidity: {humidity:.1f}% | Alarm: {alarm}")
    
            # Prepare data for CSV row
            new_data = {
                "timestamp": timestamp,
                f"{device_id}_battery": battery_mv,
                f"{device_id}_alarm": alarm,
                f"{device_id}_temp": temp_c,
                f"{device_id}_humidity": humidity
            }
    
        if device_id[:3] == "lws":
            # Unpack data (battery: uint16, temp: int16, lws: uint16, temp_leaf: int16)
            battery_mv, temp_raw, lws_raw, temp_leaf_raw = struct.unpack(">HhHh", decoded_bytes[:8])
            
            # Convert temperature and humidity
            temp_d20 = temp_raw / 10
            lws=lws_raw/10
            temp_leaf = temp_leaf_raw / 10
            if temp_d20>80: #if no optional Dallas DS20 sensor connected
                temp_d20=-999
    
            # Format the timestamp from Unix timestamp (if needed)
            # Optionally, you can use the TTN timestamp instead if it's more accurate
            #timestamp = datetime.utcfromtimestamp(unix_timestamp).isoformat()
    
            print(f"[{timestamp}] {device_id} | Battery: {battery_mv} mV | Temp: {temp_d20:.1f} °C | LWS: {lws:.1f}% | Temp: {temp_leaf:.1f} °C")
    
            # Prepare data for CSV row
            new_data = {
                "timestamp": timestamp,
                f"{device_id}_battery": battery_mv,
                f"{device_id}_temp_d20": temp_d20,
                f"{device_id}_lws": lws,
                f"{device_id}_temp_leaf": temp_leaf
            }
       
        if device_id[:11] == "temperature":
            # Decode battery and temperature
            battery_mv, temperature_raw = struct.unpack(">Hh", decoded_bytes[:4])
            temperature_c = temperature_raw / 10
        
            # Print the results
            print(f"[{timestamp}] {device_id} | Battery: {battery_mv} mV | Temp: {temperature_c:.1f} °C")
        
            
            # Create a row with NaNs for other devices
            new_data = {
                "timestamp": timestamp,
                f"{device_id}_battery": battery_mv,
                f"{device_id}_temp": temperature_c
            }
    
        if device_id[:1] == "u": #Anemometer sensor
           #example: of a measurement with an ATMOS 22 with u,dir,gust,T (it was not sending x,y)
           #hex_string = "0D5001312B302E37382B32382E332B312E35342B32332E30"
           #byte_data = bytes.fromhex(hex_string)
           #print(byte_data)  # Output: b'\rP\x011+0.78+28.3+1.54+23.0'
   
           #decoded_bytes=bytes.fromhex("0D5001312B302E37382B32382E332B312E35342B32332E30")
           
           # Skip the first 4 bytes
           data_section = decoded_bytes[4:] #was 6
           print("data_section:",data_section) #for testing
           # Decode to ASCII, clean up nulls and line endings
           ascii_data = data_section.decode(errors='ignore').strip('\r\n\x00')
           print("ascii_data:"+ascii_data)  #for testing          
           # Split by '+' or '-' but keep the signs with the numbers
           variables = re.split(r'(?=[+-])', ascii_data)
           print("variables:",variables) #test if correct data has been received
           # Parse values
           if len(variables)>=3: #avoids problems if anemometer is disconnected
               wind_speed = float(variables[1])
               wind_direction = float(variables[2])
               wind_gust = float(variables[3])
               
               if len(variables)>=6: #ATMOS 22
                   u_temp = float(variables[4])
                   x = float(variables[5])
                   y = float(variables[6])
               else: #DS-2
                   u_temp=-999
                   x = -999
                   y = -999
           else: #if sensor broken
               wind_speed = -999
               wind_direction = -999
               wind_gust = -999
               u_temp=-999#float('nan') does not work
               x = -999
               y = -999                
                                         
           # Print results
           print(f"[{timestamp}] {device_id} | Wind Speed: {wind_speed:.2f} m/s | Dir.: {wind_direction:.1f}° | wind gust: {wind_gust:.2f} m/s| T: {u_temp:.2f}°C | x: {x:.2f}° | y: {y:.2f}°")
   
           new_data = {
               "timestamp": timestamp,
               f"{device_id}_u": wind_speed,
               f"{device_id}_dir": wind_direction,
               f"{device_id}_gust": wind_gust,
               f"{device_id}_t": u_temp,
               f"{device_id}_x": x,
               f"{device_id}_y": y
               }
           print(new_data) #for testing
        # Load existing data or create new
        # Get today's date in UTC using gmtime
        ###today = datetime.fromtimestamp(gmtime()) #does not work
        today= datetime.now(timezone.utc)
        today_str = today.strftime("%Y%m%d")
        yesterday = today - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y%m%d")
            
        ##mydate=strftime("%Y%m%d", gmtime())
        
        CSV_FILE = "IOT_data_"+today_str+".csv"
        CSV_FILE_yesterday = "IOT_data_"+yesterday_str+".csv"
        
        if os.path.exists(CSV_FILE):
           df = pd.read_csv(CSV_FILE)
        else:
           df = pd.read_csv(CSV_FILE_yesterday, nrows=0)  ##load only the header
           ###df = pd.DataFrame()
    
        # Append new row with aligned columns
        new_row = pd.DataFrame([new_data])
        df = pd.concat([df, new_row], ignore_index=True)
        df = df.sort_values(by="timestamp").reset_index(drop=True)
    
        # Save updated CSV with consistent column order
        df.to_csv(CSV_FILE, index=False)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, 60)
client.loop_forever()
