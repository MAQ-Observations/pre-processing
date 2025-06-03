@echo off
:loop
echo Starting script...
python "C:\AAMS\DistributedNetwork\KNP_IOT_data_collection_MQTT_v1c.py"
echo Script exited. Restarting in 5 seconds...
timeout /t 5
goto loop
