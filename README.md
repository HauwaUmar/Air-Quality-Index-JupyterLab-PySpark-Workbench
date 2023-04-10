
# Air Quality Index JupyterLab PySpark Workbench

This repository contains a Docker Compose setup for running a JupyterLab workbench with PySpark and a PostgreSQL database for analyzing air quality index data.

## Prerequisites
To run this workbench, you'll need to have Docker and Docker Compose installed on your machine. You can download them from the official Docker website:

- Docker
- Docker Compose
## Setup

1. Clone this repository to your local machine:

```bash
git clone https://github.com/yourusername/aqi-jupyterlab-workbench.git
```

2. Navigate to the repository directory:

```bash
cd aqi-jupyterlab-workbench
```

3. Create a .env file based on the default.env file:

```bash
cp .env .env
```
## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`PG_USER=myusername`

`PG_PASSWORD=mypassword`

`PG_DATABASE=mydatabase`

`PG_PORT=5432`

`JUPYTER_PORT=8888`



## Usage

1. Open a terminal and navigate to the root of the repository

2. Run the following command to start the containers:
```
docker-compose --env-file .env up -d
```
3. Wait for the containers to start up (this may take a few minutes).

4. Open a web browser and navigate to http://localhost:8008/ .

5. You should see the JupyterLab interface with PySpark installed.

6. To stop the containers, run the following command:
```
docker-compose --env-file .env down 
```

## Details on code

1. First connect to the database on pgadmin and create the following table
```

CREATE TABLE IF NOT EXISTS public.coord_info
(
    coord_key integer,
    city_name character varying(100) COLLATE pg_catalog."default",
    province character varying(10) COLLATE pg_catalog."default",
    country character varying(100) COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision
)

CREATE TABLE IF NOT EXISTS public.current_gas_info
(
    date date,
    co double precision,
    no double precision,
    no2 double precision,
    o3 double precision,
    so2 double precision,
    pm2_5 double precision,
    pm10 double precision,
    nh3 double precision,
    aqi double precision,
    lon double precision,
    lat double precision,
    coord_key integer
)

CREATE TABLE IF NOT EXISTS public.historic_gas_info
(
    date date,
    co double precision,
    no double precision,
    no2 double precision,
    o3 double precision,
    so2 double precision,
    pm2_5 double precision,
    pm10 double precision,
    nh3 double precision,
    aqi double precision,
    lon double precision,
    lat double precision,
    coord_key integer
)



CREATE TABLE IF NOT EXISTS public.forecast
(
    date date,
    dayofweek integer,
    month integer,
    year integer,
    co double precision,
    no double precision,
    no2 double precision,
    o3 double precision,
    so2 double precision,
    pm2_5 double precision,
    pm10 double precision,
    nh3 double precision,
    aqi double precision,
    coord_key integer,
    prediction double precision
)

```

2. Run the following command to start the containers:
```
docker-compose --env-file .env up -d
```
3. Wait for the containers to start up (this may take a few minutes).

4. Open a web browser and navigate to http://localhost:8008/ .

5. You should see the JupyterLab interface with PySpark installed.

6. To stop the containers, run the following command:
```
docker-compose --env-file .env down 
```
## Additional Information
- The db_data volume is used to persist the data in the PostgreSQL container across restarts.
- The ./notebooks directory is mounted as a volume in the JupyterLab container, so you can save your notebooks there and they will persist across container restarts.
- The ./data and ./scripts directories are also mounted as volumes in the JupyterLab container, so you can access them from within the container.
- The jupyterlab service is built from the ./jupterspark directory, which contains a Dockerfile that installs PySpark and other dependencies in the JupyterLab container. You can modify this file as needed to add additional dependencies.
