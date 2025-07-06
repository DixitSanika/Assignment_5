# Assignment_5
# Data Pipeline Assignment – Week 5

This is my Week 5 assignment for the Data Engineering Internship. In this assignment, I worked on building a small data pipeline using Python and MySQL. The project is divided into four parts, each handled through Python scripts and MySQL. 

# Part A: Export Data to Multiple Formats
Extract data from a MySQL database and save it in three different formats: CSV, Parquet, and Avro.
- I have used pandas for data handling.
- I have used pyarrow and fastavro for working with Parquet and Avro formats.

Main script: export_data.py

# Part B: Scheduled or Trigger-Based Automation
Introduce scheduling or trigger logic to simulate automated pipelines.
- I have implemented script-based scheduling to run tasks at defined intervals.

Scripts: event_trigger.py and run_schedule.py

# Part C: Full and Selective Table Replication
Copy data between MySQL databases.
- I have implemented full table copying from one database to another.

Scripts: export_all_tables.py and project_copy.py

# Part D: Copy Selective Tables with Selective Columns
This part of the assignment focuses on copying specific tables with selected columns from one MySQL database to another. Instead of moving everything, this method allows more control by choosing only the data that is actually needed. 

Main script: copy_table.py

# About Me
I'm Sanika Dixit, currently working as a Data Engineering Intern.
Feel free to explore the code, suggest improvements, or reach out if you’re working on something similar. I’m always happy to connect and collaborate.

# Contact
If you have questions or feedback, you can connect with me:

- LinkedIn: www.linkedin.com/in/sanika-dixit-6a2b92259
- Email: sanikadixit80@gmail.com
