# pre-processing
Preprocessing scripts for preparing the MAQ-Observations.nl data and send it to the database. This processing happens in two steps:

1) Prepare4API: Prepares and homogenizes the raw data in static .csv files.
    - Prepare4API-live.py: Operational script running every 10 minutes.
    - Prepare4API-historical.py: Used to prepare historical data.
    - Prepare4API-functions.py: Toolbox used with all functions.
  
2) Post2API: Posts the data to the MAQ-Observations database with an ApiKey that is allowed to post data.
    - Post2API-live.py: Operational script running every 10 minutes.
    - Post2API-historical.py: Used to post historical data.
    - Post2API-functions.py: Toolbox used with all functions.

This folder also includes an example how to post data using JSON commands (PostAPI.json). Again, an ApiKey that allows posting data is needed.
