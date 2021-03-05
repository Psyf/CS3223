## Set up PostgreSQL for testing runtimes

> Make sure you are in /testcases/utils
> 
1. Create a new python env / Activate your python env
2. `pip install -r requirements.txt`
3. `python createAirlines.py`

> To run postgres

1. `docker-compose up -d`
2. Go to `localhost:8000` to use pgAdmin4 
3. Login: admin@u.nus.edu Password: password
4. Select "Add new server"
5. Under General, fill in any name
6. Under Connection, hostname: utils_db_1 (or whatever your postgres container is called...), port: 5432, maintenance database: postgres, username: postgres, password: password