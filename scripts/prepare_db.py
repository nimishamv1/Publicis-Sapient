import psycopg2

def create_weather_table():
    try:
        # Connect to your PostgreSQL database
        connection = psycopg2.connect(
            dbname="your_db_name",
            user="your_username",
            password="your_password",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()

        # Create table query
        create_table_query = '''
        CREATE TABLE weather_data (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            temperature FLOAT NOT NULL,
            humidity FLOAT NOT NULL,
            precipitation FLOAT,
            wind_speed FLOAT
        );
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