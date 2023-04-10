
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

## Additional Information
- The db_data volume is used to persist the data in the PostgreSQL container across restarts.
- The ./notebooks directory is mounted as a volume in the JupyterLab container, so you can save your notebooks there and they will persist across container restarts.
- The ./data and ./scripts directories are also mounted as volumes in the JupyterLab container, so you can access them from within the container.
- The jupyterlab service is built from the ./jupterspark directory, which contains a Dockerfile that installs PySpark and other dependencies in the JupyterLab container. You can modify this file as needed to add additional dependencies.
