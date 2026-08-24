
# Classroom Air Quality Monitoring

## Table of Contents
1. [Context](https://github.com/LuisBarbosa02/Classroom-Air-Quality-Monitoring#context)
2. [How to Use](https://github.com/LuisBarbosa02/Classroom-Air-Quality-Monitoring#how-to-use)

## Context
This project implements real-time data streaming for monitoring the air quality in a classroom, based on the Kaggle dataset "Classroom Air Quality."

## How to Use
### Installation
This repository requires Python 3.12.12

Clone and change to the repository:
```bash
git clone https://github.com/LuisBarbosa02/Classroom-Air-Quality-Monitoring.git
cd Classroom-Air-Quality-Monitoring
```

[Optional] Create and activate a virtual environment:
```bash
python3.12 -m venv venv
source venv/bin/activate
```

### Running Project
Inside the *Classroom-Air-Quality-Monitoring* folder, build the Docker images from the docker-compose.yml file:
```bash
docker compose build
```

Then, run all of the project containers through:
```bash
docker compose up -d
```

Verify if data is being saved into PostgreSQL:
```bash
docker exec -it my-postgres psql -U myuser -d mydb
SELECT COUNT(*) FROM classroom_air_quality;
```

Shut down the Docker Compose service:
```bash
docker compose down
```
