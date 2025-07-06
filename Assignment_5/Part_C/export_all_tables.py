import os
import pandas as pd
from sqlalchemy import create_engine, text
import fastavro

# Creating an output folder to store the exported files
output_dir = "exports"
os.makedirs(output_dir, exist_ok=True)

# Set up the database connection using SQLAlchemy
print("Connecting to MySQL database")
engine = create_engine("mysql+pymysql://root:Sanika%40123@localhost/project")

try:
    with engine.connect() as conn:
        print("Connection successful.")
        print("Fetching the available table names")

        # Get all table names from the connected database
        result = conn.execute(text("Show tables"))
        tables = [row[0] for row in result]
        print(f"Found tables: {tables}")

        for table in tables:
            print(f"\nExporting data from table: {table}")
            df = pd.read_sql(f"select * from {table}", conn)

            # Export to CSV
            csv_path = os.path.join(output_dir, f"{table}.csv")
            df.to_csv(csv_path, index=False)
            print(f"CSV file saved: {csv_path}")

            # Export to Parquet
            parquet_path = os.path.join(output_dir, f"{table}.parquet")
            df.to_parquet(parquet_path, index=False)
            print(f"Parquet file saved: {parquet_path}")

            # Prepare the data for Avro export
            avro_path = os.path.join(output_dir, f"{table}.avro")
            records = df.where(pd.notnull(df), None).to_dict(orient="records")

            # Avro schema
            schema = {
                "doc": f"{table} export",
                "name": table,
                "namespace": "example.avro",
                "type": "record",
                "fields": [
                    {
                        "name": col,
                        "type": ["null", "int"] if pd.api.types.is_integer_dtype(df[col])
                        else ["null", "float"] if pd.api.types.is_float_dtype(df[col])
                        else ["null", "string"]
                    }
                    for col in df.columns
                ]
            }

            # Write to Avro file
            with open(avro_path, "wb") as out:
                fastavro.writer(out, schema, records)
            print(f"Avro file saved: {avro_path}")

        print("\nAll the tables are exported successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

print("Export process completed.")
