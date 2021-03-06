import psycopg2

# for running tests so credentials dont have to be protected
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASS = 'password'
DB_HOST = 'localhost'

try: 
    conn = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} host={DB_HOST} password={DB_PASS}")
except:
    print("Unable to connect to database!")

cur = conn.cursor()

def insert_data(tablename, filename, headers):
    with open(filename, 'r') as reader:
        columns = ",".join(headers)
        placeholders = ",".join(["%s" for i in range(0, len(headers))])
        query = f'INSERT INTO {tablename} ({columns}) VALUES ({placeholders})'
        print(query)

        rows = []
        header, line = reader.readline(), reader.readline()
        while line != "":
            row = line.split(" ")[0].split("\t")
            row = tuple(row[:len(row)-1])
            rows.append(row)
            # print(str(line.split(' ')[0]))
            line = reader.readline()

    print(f'Inserting into {tablename}...')
    try: 
        cur.executemany(query, rows)
    except Exception as err:
        print(err)
    finally:
        conn.commit()
    print(f'Done')
    return

if __name__ == "__main__":
    insert_data("certified", 'certified.out', ["eid", "aid"])
    insert_data("schedule", 'schedule.out', ["flno", "aid"])
    insert_data("employees", 'employees.out', ["eid", "ename", "salary"])
    insert_data("flights", 'flights.out', ["flno", "camefrom", "goingto", "distance", "departs", "arrives"])
    insert_data("aircrafts", 'aircrafts.out', ["aid", "aname", "cruisingrange"])
    cur.close()
    conn.close()




    

