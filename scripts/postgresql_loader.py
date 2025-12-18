import pandas as pd
from sqlalchemy import create_engine

# Database connection parameters
db_user = 'weatherapi_login'
db_password = "Sapient"
db_host = '34.126.198.60'
db_port = '5432'
db_name = 'postgres'
table_name = 'weather_data'

# Create a database connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load CSV file
csv_file_path = 'microclimate_20.csv'
data = pd.read_csv(csv_file_path)

# Load data into PostgreSQL table
data.to_sql(table_name, engine, if_exists='replace', index=False)

print("Data loaded successfully.")