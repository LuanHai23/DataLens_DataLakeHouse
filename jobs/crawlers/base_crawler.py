from datetime import datetime
import json
import os

class BaseCrawler:
    def __init__(self, source_name):
        self.source_name = source_name
        self.output_dir = f"data/raw/{source_name}/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def save_to_json(self, data):
        if not data:
            print(f"[{self.source_name}] No data to save.")
            return
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.output_dir}/job_{timestamp}.json"

        with open(filename, 'w') as f:
            json.dump(data, f)

        print(f"[{self.source_name}] Saved data to {filename}")