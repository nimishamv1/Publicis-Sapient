import pandas as pd
from sqlalchemy import create_engine

# Database connection parameters
db_user = 'your_username'
db_password = 'your_password'
db_host = 'localhost'
db_port = '5432'
db_name = 'your_database'
table_name = 'your_table'

# Create a database connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load CSV file
csv_file_path = 'path_to_your_csv_file.csv'
data = pd.read_csv(csv_file_path)

# Load data into PostgreSQL table
data.to_sql(table_name, engine, if_exists='replace', index=False)

print("Data loaded successfully.")