import os
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
part_a_path = os.path.join(parent_dir, 'Part_A')
sys.path.append(part_a_path)

from export_data import export_data  # type:ignore

watch_folder = os.path.join(os.path.dirname(__file__), 'watch_folder')

class EventTrigger(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            print(f"New file detected: {event.src_path}")
            print("Triggering export process")
            export_data()

if __name__ == "__main__":
    if not os.path.exists(watch_folder):
        os.makedirs(watch_folder)

    print(f"Monitoring folder: {watch_folder}")
    event_handler = EventTrigger()
    observer = Observer()
    observer.schedule(event_handler, path=watch_folder, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Monitoring stopped.")
    observer.join()
