# CNN-LOF
 Lightweight Operational Framework for Forecasting Streamflow using Convolutional Neural Networks

Slides [here](https://www.researchgate.net/publication/384365455_Lightweight_Operational_Framework_for_Forecasting_Streamflow_using_Convolutional_Neural_Networks?_tp=eyJjb250ZXh0Ijp7ImZpcnN0UGFnZSI6ImhvbWUiLCJwYWdlIjoicHJvZmlsZSIsInByZXZpb3VzUGFnZSI6ImhvbWUiLCJwb3NpdGlvbiI6InBhZ2VDb250ZW50In19)

## Abstract

Streamflow forecasting is a critical component of water resources management. Accurate streamflow forecasts can help water resource managers to make informed decisions about water allocation and to mitigate the impacts of floods and droughts. In the last decade, Machine Learning (ML) has shown great potential in forecasting streamflow. The availability of large amounts of data and advances in computational power have enabled the development of these models, either in high-performance computing environments or in the cloud. However, the accessibility of these models to non-experts in ML and the complexity of the computational infrastructure required to run them are still a challenge. ML and the complexity of the computational infrastructure required to run them are still a challenge for low-spec devices. 

We propose a lightweight operational system for forecasting mean and maximum daily streamflow up to 5 days in advance for hundreds of gauge stations across continental Chile using Convolutional Neural Networks (CNN). In contrast to other ML models, such as LSTM, CNN handles small datasets, an advantage when working with low-spec devices. This framework aims to cover all the stages of the forecasting process, from data downloading to reporting, in a simple and automated way. The computational infrastructure required to run this framework is minimal. To test these requirements, this framework currently runs on a quad-core with 8 GB of RAM low-cost device ($75 USD Raspberry Pi 4B). Since this system is intended for operational application by non-experts users, only a simple initial configuration is required.

With this framework, we aim to democratize the use of ML for streamflow forecasting, making it accessible to non-experts in ML and to small water resource management organizations.

**Note**: since GitHub limits the size of the files that can be uploaded, the trained models are not included in this repository. For now, send me an email if you want to access the trained models. I will find a workaround to make them available.

## Installation

**Minimum Python version:** 3.10

Install the libraries from the requirements file:

```bash
pip install -r requirements.txt
```

The following repositories contains in detail the downloading process for the data used in this project:

 - [HidroCL-OOP](https://github.com/MeteorologiaUV/HidroCL-OOP)
 - [HidroCL-WaterBodyArea](https://github.com/MeteorologiaUV/HidroCL-WaterBodyArea)

Clone the repository where you want to run the pipeline. Then, for running the core process use 
`./src/run_process.py` file, which includes from the data downloading to model inference.

**TODO:**

 - [ ] Add the refined report generation

## Configuration before running the pipeline

Before running the pipeline, take a look into the `.env` file example provided to set up some credentials, paths and configurations. The system loads the environment variables from the `.env` file using the `python-dotenv` library.

Also, for the downloading process, the following credentials are required:


### Climate Data Store (CDS)

For using the Climate Data Store (CDS), you need to sign up for an account [here](https://cds.climate.copernicus.eu/). After signing up, you need create a `.cdsapirc` file in your home directory and add the following lines:

```
url: https://cds-beta.climate.copernicus.eu/api
key: keyvalue
```

### EarthData credentials

`.netrc`: The `.netrc` file is a file in your home directory that contains login and password information for remote servers. The `.netrc` file is used by the `wget` command to authenticate to the Earthdata Login system. The `.netrc` file should be created in your home directory and should have the following format:

```
machine urs.earthdata.nasa.gov
    login username
    password password
```

### Google Earth Engine (GEE)

For using GEE, you need to have a Google account and sign up for GEE. You can sign up for GEE [here](https://earthengine.google.com/). Also, after setting up the Google Cloud Platform account, you need to create a service account key and set the environment variable `GEE_JSON` to the path of the service account key and the environment variable `SERVICE_ACC` with the servivce account email. 

### IMERG credentials

For using the IMERG data, you need to sign up for an account [here](https://registration.pps.eosdis.nasa.gov/registration/). Then, add the user and password to the .env file (`IMERG_USER` and `IMERG_PWD`).
