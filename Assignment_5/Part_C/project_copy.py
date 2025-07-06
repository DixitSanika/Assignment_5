import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

source_user = 'root'
source_password = quote_plus('Sanika@123')
source_host = 'localhost'
source_db = 'project'

target_user = 'root'
target_password = quote_plus('Sanika@123')
target_host = 'localhost'
target_db = 'project_copy'

# Creating database engine connections using SQLAlchemy
source_engine = create_engine(
    f"mysql+pymysql://{source_user}:{source_password}@{source_host}/{source_db}"
)
target_engine = create_engine(
    f"mysql+pymysql://{target_user}:{target_password}@{target_host}/{target_db}"
)

# List of tables and the specific columns:
tables_to_copy = {
    'data1': ['ID', 'Name'],
}

for table_name, columns in tables_to_copy.items():
    try:
        print(f"Copying selected columns from '{table_name}'")

        query = f"select {', '.join(columns)} from {table_name}"
        df = pd.read_sql(query, source_engine)

        # Writing the selected data to the target database
        df.to_sql(table_name, target_engine, if_exists='append', index=False)

        print(f"Copied {len(df)} rows from '{table_name}' to '{target_db}'.\n")

    except Exception as e:
        print(f"Failed to copy from '{table_name}': {e}\n")
