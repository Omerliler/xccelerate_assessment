# Xccelerated Dream Shop

Data Engineer Assessment 



## Installation
### Prerequisites
- Python 3.9
- Docker 4.25


### How to download
```bash
## Clone the project
git clone https://github.com/yourusername/yourproject.git
cd yourproject

```
### How to Setup
```bash
# Pull postgres
docker pull postgres

# Create postgresql container
docker run --name postgresql -e "POSTGRES_USER=omerliler" -e "POSTGRES_PASSWORD=admin" -p 5432:5432 -v /data:/var/lib/postgresql/data -d postgres

# Pull PgAdmin to setup postgres
docker pull dpage/pgadmin4:latest

# Create PgAdmin container
docker run --name my-pgadmin -p 82:80 -e "PGADMIN_DEFAULT_EMAIL=omerliler@test.com" -e "PGADMIN_DEFAULT_PASSWORD=admin" -d dpage/pgadmin4

```

Open the browser and go to http://localhost:82/


Create a server 

### How to Run

```bash
## Activate virtual env
envxcc\Scripts\Activate 

# Install requirements
pip install -r requirements.txt

# Run ETL script 
python xccelerated_assessment.py

# Run API
python flask_api.py
```

### Endpoint 
```bash
curl "http://localhost:5050/metrics/orders"

# Response:
{
    "median_session_duration_minutes_before_order": 27.5,
    "median_visits_before_order": 2.0
}
```






