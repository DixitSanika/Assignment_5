import sys
import os
import schedule
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Part_A.export_data import export_data

def run():
    print("Running scheduled export")
    export_data()

schedule.every(10).minutes.do(run)

print("Schedule has started")
while True:
    schedule.run_pending()
    time.sleep(1)
