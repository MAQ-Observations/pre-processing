@echo off
:loop
echo Starting script...
python "C:\AAMS\DistributedNetwork\IOT_sensor_logging_MQTT_com.py"
echo Script exited. Restarting in 5 seconds...
timeout /t 5
goto loop
