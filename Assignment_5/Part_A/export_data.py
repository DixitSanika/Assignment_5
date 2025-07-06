# To export the csv, parquet and avro files, python libraries like pandas, pyarrow and fastavro are used here.
 
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
from sqlalchemy import create_engine

def export_data():
    try:
        print("Establishing connection to MySQL database")
        
        # Creating a connection to the MySQL database using SQLAlchemy
        engine = create_engine(
            "mysql+pymysql://root:Sanika%40123@127.0.0.1:3306/project",
            connect_args={"connect_timeout": 5}
        )
        conn = engine.connect()
        print("Connection established successfully")
        
        # Reading data from the data1 table into a pandas dataframe
        df = pd.read_sql("Select * from data1", conn)
        print("Data retrieved. Columns found:", df.columns.tolist())
        
        # Exporting the dataframe to a CSV file
        df.to_csv("data1.csv", index=False)
        print("Data exported to CSV format.")
        
        # Exporting the dataframe to a Parquet file
        table = pa.Table.from_pandas(df)
        pq.write_table(table, "data1.parquet")
        print("Data exported to Parquet format.")
        
        # Convert column types for Avro
        df['ID'] = df['ID'].astype(int)
        df['Age'] = df['Age'].astype(int)
        df['Name'] = df['Name'].astype(str)
        df['Email_id'] = df['Email_id'].astype(str)

        records = df.to_dict(orient="records")
        
        # Avro Schema
        schema = {
            "doc": "data1 table",
            "name": "DataRecord",
            "namespace": "example.avro",
            "type": "record",
            "fields": [
                {"name": "ID", "type": "int"},
                {"name": "Name", "type": "string"},
                {"name": "Email_id", "type": "string"},
                {"name": "Age", "type": "int"}
            ]
        }
        
        # Writing the records to an Avro file
        with open("data1.avro", "wb") as out_file:
            fastavro.writer(out_file, schema, records)
        print("Data exported to Avro format.")

    except Exception as e:
        print("An error occurred during export:", e)

    # Closing the DB connection
    finally:
        if 'conn' in locals():
            conn.close()
            print("Connection to MySQL closed.")

if __name__ == "__main__":
    export_data()
