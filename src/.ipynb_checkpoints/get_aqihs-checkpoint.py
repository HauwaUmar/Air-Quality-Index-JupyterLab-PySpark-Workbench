# importing requests and json
import requests, json
import pandas as pd
import datetime
import psycopg2
import pandas as pd
import psycopg2.extras as extras
from sqlalchemy import create_engine
    
#### inserts data into table 
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
    host='159.203.40.51',
    port='5432'
)

  
# creating a cursor
cursor = conn.cursor()

#cursor.execute('''TRUNCATE TABLE current_gas_info''')



#Retrieving data
cursor.execute('''SELECT * from coord_info''')

result = cursor.fetchall();

# changing coordinate information from tuple to dataframe
data = pd.DataFrame(result, columns=['coord_key',"city_name", "province", "country", "latitude","longitude"])

# API key 
API_KEY = "bc83258f0da0d1c1f4bdbf5b54261172"

df = pd.DataFrame(columns=['dt', 'co', 'no','no2','o3','so2','pm2_5','pm10','nh3','aqi','lon','lat','coord_key'])
count =0
for ind in data.index:
    lat = str(data['latitude'][ind])
    lon = str(data['longitude'][ind])
    BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution/history?lat="+lat+"&lon="+lon+"&start=1665288000&end=1681012800&appid=" + API_KEY
    response = requests.get(BASE_URL)
    info = []
    coord_info = []
    # checking the status code of the request
    if response.status_code == 200:
    # getting data in the json format
        # getting data in the json format
        data2 = response.json()
        for k,v in data2.items():
                if k == 'coord':
                    for a,t in v.items():
                        coord_info.append(t)
                        print(t)
                if k != 'coord':
                    for f in v:
                        info.append(f)
        new_data = pd.DataFrame(info)
        comp = pd.json_normalize(new_data['components'])
        aqi =pd.json_normalize(new_data['main'])
        new_data['dt'] = pd.to_datetime(new_data['dt'],unit='s')
        new_data =  pd.concat([new_data['dt'], comp,aqi], axis=1)
        new_data[['lon','lat']] = lon,lat
        new_data['coord_key'] = data['coord_key'][ind]
        df = df.append(new_data, ignore_index = True)
        count=count+1
        print("City: {}".format(count))
        #print(df.head())

df = df[["dt","co","no","no2","o3","so2","pm2_5","pm10","nh3","aqi","lon","lat","coord_key"]]

df.rename(columns = {'dt':'date'}, inplace = True)  


# using the function defined
execute_values(conn, df, 'historic_gas_info')