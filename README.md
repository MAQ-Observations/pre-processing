[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16811525.svg)](https://doi.org/10.5281/zenodo.16811525)

# pre-processing
Main preprocessing for preparing the MAQ-Observations.nl data and send it to the database. This main processing happens in two steps:

1) Prepare4API: Prepares and homogenizes the raw data in static .csv files.
    - Prepare4API-live.py: Operational script running every 10 minutes.
    - Prepare4API-historical.py: Used to prepare historical data.
    - Prepare4API-functions.py: Toolbox used with all functions.
  
2) Post2API: Posts the data to the MAQ-Observations database with an ApiKey that is allowed to post data.
    - Post2API-live.py: Operational script running every 10 minutes.
    - Post2API-historical.py: Used to post historical data.
    - Post2API-functions.py: Toolbox used with all functions.

This folder also includes an example how to post data using JSON commands (PostAPI.json) or remove datastreams through Python Requests. Again, an ApiKey that allows posting data or removing data is needed.

Additionally, this repository contains the 'DistributedNetwork' folder. This is a stand-alone process that retrieves the data through IoT. The data goes either through a public open-source network or KPN network depending on the signal of the open-source network. This data is directly combined, processed and send the the MAQ-Observations database unlike the main data-flow.

How to cite this material:
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16811525.svg)](https://doi.org/10.5281/zenodo.16811525)
