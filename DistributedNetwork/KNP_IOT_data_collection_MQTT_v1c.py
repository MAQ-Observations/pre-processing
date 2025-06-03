# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 10:44:27 2025

@author: heusi001

Subscribed to Broker EMQX for MQTT service
https://cloud-intl.emqx.com/console/deployments/g669e111/overview

MQTT Client ID kpn-cleint-001
Topic uplink/devices/${DevEUI}
(it was default at kpn: kpnthings/%c/%n)

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
import json
import re
import base64
import struct

from datetime import datetime
import pandas as pd
import os

from time import gmtime, strftime, time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt



HOST = "g669e111.ala.us-east-1.emqxsl.com"
PORT = 8883
USERNAME = "bert"
PASSWORD = "7AsaVAnJiMXvp96"
TOPIC = "uplink/devices/#"
#TOPIC = "testtopic/#"
output_dir = "C:\AAMS_data\DistributedNetwork\\"

def on_connect(client, userdata, flags, rc):
    print("✅ Connected with result code", rc)
    client.subscribe(TOPIC)
####

def extract_dev_eui(message):
    try:
        data = json.loads(message)
        for entry in data:
            if "bn" in entry:
                match = re.search(r"DEVEUI:([A-Fa-f0-9]+)", entry["bn"])
                if match:
                    return match.group(1)
        return None
    except Exception as e:
        print("Error parsing DevEUI:", e)
        return None

def extract_hex_payload(message):
    try:
        data = json.loads(message)
        for entry in data:
            if entry.get("n") == "payload" and "vs" in entry:
                return entry["vs"]
        return None
    except Exception as e:
        print("Error extracting hex payload:", e)
        return None

####


def on_message(client, userdata, message):
    #print(f"📦 Topic: {message.topic}")
    #print(f"📨 Payload: {message.payload.decode()}")
######
    # Parse JSON payload
    ##data = json.loads(message.payload.decode())


    #Extract uplink_message
    ##uplink = data["uplink_message"]
    print(f"\n📦 Topic: {message.topic}")
    payload = message.payload.decode()
    print(f"📨 Raw Payload: {payload}")
    


    # --- Get timestamp from TTN or fallback to local time ---
    ##ttn_timestamp = uplink.get("received_at")
    ##timestamp = ttn_timestamp if ttn_timestamp else datetime.utcnow().isoformat()
    timestamp=datetime.utcnow().isoformat()
    
    # Get and decode frm_payload (base64)
    ##b64_payload = uplink["frm_payload"]
    
    #b64_payload=extract_base64_payload(payload) #not b64 for kpn
    #decoded_bytes = base64.b64decode(b64_payload) #not b64 for kpn
    
    hex_payload = extract_hex_payload(payload)
    if not(hex_payload==None):  #sometimes the sensor data payload is empty
        decoded_bytes = bytes.fromhex(hex_payload)  
        #byte_length = len(decoded_bytes)
        ##Device ID needs to be assigned to a sensor type+number (TTN does not need that)
        ##so for each new sensor one needs to add that to the list
        ##for future version this should be moved to a separate file with column device_id_dev_eui and column device_id
        device_id_dev_eui=extract_dev_eui(payload)
        print("device_id_de_eui="+device_id_dev_eui)
        '''
        if device_id_dev_eui=="A84041B9E15A8186": 
            device_id="lws2-kpn"
            print("device_id= "+device_id)
        ##device_id = data["end_device_ids"]["device_id"]
        if device_id_dev_eui=="A8404143C0591E67": 
            device_id="u2-kpn"
            print("device_id= "+device_id)
        '''
        df=pd.read_csv("device_list.txt")
        result = df[df['DEV_EUI'] == device_id_dev_eui]
        device_id=result.name.values[0]
        
        
        
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
            battery_mv, temp_raw, lws_raw, temp_leaf_raw = struct.unpack(">HhHh", decoded_bytes[:-3])
            
            # Convert temperature and humidity
            temp_d20 = temp_raw / 10
            lws=lws_raw/10
            temp_leaf = temp_leaf_raw / 10
    
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
       
        if device_id[:4] == "temp":
            # Decode battery and temperature
            #example data: 0fae00f500000c7fff7fff 
            #battery: 4014 mV
            #Temperature 245*0.1=24.5C
            #output 2 bytes battery, 2 bytes temp, two bytes ignore, 1 byte alarm, 2 /,2 /
            #
            
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
            # Skip the first 4 bytes
            data_section = decoded_bytes[4:] #was 6
            
            # Decode to ASCII, clean up nulls and line endings
            ascii_data = data_section.decode(errors='ignore').strip('\r\n\x00')
            
            # Split by '+' or '-' but keep the signs with the numbers
            variables = re.split(r'(?=[+-])', ascii_data)
            
            # Parse values
            if len(variables)>=3: #avoids problems if anemometer is disconnected
                wind_speed = float(variables[1])
                wind_direction = float(variables[2])
                wind_gust = float(variables[3])
                
                if len(variables)>=6: #ATMOS 22
                    u_temp = float(variables[4])
                    x = float(variables[5])
                    y = float(variables[6])
                else:
                    u_temp=float('nan')
                    x = float('nan')
                    y = float('nan')
            else:
                wind_speed = float('nan')
                wind_direction = float('nan')
                wind_gust = float('nan')
                u_temp=float('nan')
                x = float('-9999')
                y = float('-9999')                
                                          
            # Print results
            #print(f"Wind Speed: {wind_speed} m/s")
            #print(f"Wind Direction: {wind_direction}°")
            #print(f"Wind Gust: {wind_gust} m/s")
            print(f"[{timestamp}] {device_id} | Wind Speed: {wind_speed:.2f} m/s | Dir.: {wind_direction:.1f}° | wind gust: {wind_gust:.2f} m/s| T: {u_temp:.2f}°C | x: {x:.2f}° | y: {y:.2f}°")

            new_data = {
                "timestamp": timestamp,
                f"{device_id}_u": wind_speed,
                f"{device_id}_dir": wind_direction,
                f"{device_id}_gust": wind_gust,
                f"{device_id}_T": u_temp,
                f"{device_id}_x": x,
                f"{device_id}_y": y
                }
        # Load existing data or create new
        # Get today's date in UTC using gmtime
        ###today = datetime.fromtimestamp(time()) #this is local time!
        today= datetime.now(timezone.utc)
        today_str = today.strftime("%Y%m%d")
        yesterday = today - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y%m%d")
            
        ##mydate=strftime("%Y%m%d", gmtime())
        
        CSV_FILE = output_dir+"IOT_data_kpn_"+today_str+".csv"
        CSV_FILE_yesterday = output_dir+"IOT_data_kpn_"+yesterday_str+".csv"
        
        if os.path.exists(CSV_FILE):
           df = pd.read_csv(CSV_FILE)
        else:
           df = pd.read_csv(CSV_FILE_yesterday, nrows=0)  ##load only the header
           ##df = pd.DataFrame()
            
        # Append new row with aligned columns
        new_row = pd.DataFrame([new_data])
        df = pd.concat([df, new_row], ignore_index=True)
        df = df.sort_values(by="timestamp").reset_index(drop=True)
    
        # Save updated CSV with consistent column order
        df.to_csv(CSV_FILE, index=False)
   
######
client = mqtt.Client()
client.tls_set()  # Uses system CA certs (EMQX default)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT)
client.loop_forever()
