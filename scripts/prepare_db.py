import psycopg2

def create_weather_table():
    try:
        # Connect to your PostgreSQL database
        connection = psycopg2.connect(
            dbname="postgres",
            user="weatherapi_login",
            password="Sapient",
            host="34.126.198.60",
            port="5432"
        )
        cursor = connection.cursor()

        # Create table query
        create_table_query = '''
        CREATE TABLE weather_data (
            device_id VARCHAR(255) NOT NULL,
            received_at TIMESTAMP NOT NULL,
            sensorlocation VARCHAR(255) NOT NULL,
            latlong VARCHAR(255),
            minimumwinddirection DOUBLE PRECISION,
            averagewinddirection DOUBLE PRECISION,
            maximumwinddirection DOUBLE PRECISION,
            minimumwindspeed	DOUBLE PRECISION,
            averagewindspeed	DOUBLE PRECISION,
            gustwindspeed	DOUBLE PRECISION,
            airtemperature DOUBLE PRECISION,
            relativehumidity DOUBLE PRECISION,
            atmosphericpressure	DOUBLE PRECISION,
            pm25	DOUBLE PRECISION,
            pm10	DOUBLE PRECISION,
            noise	DOUBLE PRECISION
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_location_timestamp
ON weather_data (sensorlocation, received_at);
        '''

        # Execute the query
        cursor.execute(create_table_query)
        connection.commit()
        print("Table weather_data created successfully")

    except Exception as error:
        print(f"Error creating table: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_weather_table()