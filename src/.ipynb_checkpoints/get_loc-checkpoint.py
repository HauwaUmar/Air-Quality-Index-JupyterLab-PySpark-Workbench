import pandas as pd
import psycopg2
import pandas as pd
import psycopg2.extras as extras
from sqlalchemy import create_engine
  
def execute_values(conn, df, table):
      
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
  
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()
  
  
# establishing connection
conn = psycopg2.connect(
    database="data6300",
    user='aqidb',
    password='mypassword',
    host='host.docker.internal',
    port='5432'
)

  
# creating a cursor
cursor = conn.cursor()
data = pd.read_csv("coord_info2.csv")
  
data = data[["coord_key","city_name", "province", "country", "latitude","longitude"]]
  
# using the function defined
execute_values(conn, data, 'coord_info')