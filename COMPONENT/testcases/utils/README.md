## Set up PostgreSQL for testing runtimes

> To run postgres

1. `docker-compose up -d`
2. `docker exec -it utils_db_1 bash`
3. `psql -U postgres -d postgres`
4. Paste SQL statements from `db.sql`
5. Go to `localhost:8000` to use pgAdmin4 
6. Login: admin@u.nus.edu Password: password
7. Select "Add new server"
8. Under General, fill in any name
9. Under Connection, hostname: utils_db_1 (or whatever your postgres container is called...), port: 5432, maintenance database: postgres, username: postgres, password: password

> Make sure you are in /testcases/utils
> 
1. Create a new python env / Activate your python env
2. `pip install -r requirements.txt`
3. `python createAirlines.py`

