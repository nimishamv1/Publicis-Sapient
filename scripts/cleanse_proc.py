import psycopg2
from psycopg2 import sql
import sys

def create_stored_procedure(host, database, user, password):
    """Create a stored procedure in PostgreSQL"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port="5432"
        )
        cursor = conn.cursor()
        
        # SQL to create stored procedure
        create_proc_sql = """
            CREATE OR REPLACE PROCEDURE
            cleansed_weather_data()
            LANGUAGE plpgsql AS $$
            DECLARE
            deleted_count INTEGER;
            BEGIN
            -- Deleting duplicate records based on received_at and sensorlocation
            WITH
            CTE AS (
            SELECT
                "device_id",
                "received_at",
                "sensorlocation",
                "airtemperature",
                "relativehumidity",
                ROW_NUMBER() OVER (PARTITION BY "received_at", "sensorlocation" ORDER BY "device_id") AS RowNum
            FROM
                "public"."weather_data" )
            DELETE
            FROM
            "public"."weather_data"
            WHERE
            ("received_at",
                "sensorlocation") IN (
            SELECT
                "received_at",
                "sensorlocation"
            FROM
                CTE
            WHERE
                RowNum > 1 ); GET DIAGNOSTICS deleted_count = ROW_COUNT; RAISE NOTICE 'Deleted % duplicate records.',
            deleted_count;
            -- Deleting NULL values in sensorlocation
            DELETE
            FROM
            "public"."weather_data"
            WHERE
            "sensorlocation" IS NULL; GET DIAGNOSTICS deleted_count = ROW_COUNT; RAISE NOTICE 'Deleted % records with NULL sensorlocation.',
            deleted_count;
            -- You can add more deletion steps and print statements as needed.
            END
            ; $$;
        """
        
        cursor.execute(create_proc_sql)
        conn.commit()
        print("Stored procedure created successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def call_stored_procedure(host, database, user, password):
    """CALL a stored procedure in PostgreSQL"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port="5432"
        )
        cursor = conn.cursor()
        
        # SQL to create stored procedure
        call_proc_sql = """
            CALL cleansed_weather_data();
        """

        cursor.execute(call_proc_sql)
        conn.commit()
        print("Stored procedure called successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_stored_procedure(
        host="34.126.198.60",
        database="postgres",
        user="weatherapi_login",
        password=sys.argv[1]
    )
    call_stored_procedure(
        host="34.126.198.60",
        database="postgres",
        user="weatherapi_login",
        password=sys.argv[1]
    )