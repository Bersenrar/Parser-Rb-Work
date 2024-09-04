from pymongo import MongoClient
from datetime import datetime
import pandas as pd


class DataBaseManager:

    def __init__(self, conn_url="mongodb://localhost:27017", db_name="parsed_resumes", collection_name="parsing_results"):
        self.conn_url = conn_url
        self.db_name = db_name
        self.collection_name = collection_name

    def __enter__(self):
        self.client = MongoClient(self.conn_url)
        self.db = self.client.get_database(self.db_name)
        self.collection = self.db.get_collection(self.collection_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def append_data(self, date_key, resource_name, query, data):
        document = self.collection.find_one({"_id": date_key})

        if not document:
            new_document = {
                "_id": date_key,
                resource_name: {
                    query: data
                }
            }
            self.collection.insert_one(new_document)
        else:
            update_query = {
                "$set": {f"{resource_name}.{query}": data}
            }
            self.collection.update_one({"_id": date_key}, update_query)

    def fetch_data(self, date_key):
        document = self.collection.find_one({"_id": date_key})
        return document

    def fetch_all(self):
        documents = list(self.collection.find())
        return documents

    def fetch_all_ids(self):
        ids = self.collection.find({}, {"_id": 1})
        return [doc["_id"] for doc in ids]


class MarksManager:
    def __init__(self):
        self.weights_table = {
            "education": 0.3,
            "working_experience": 0.4,
            "skills": 0.1,
            "languages": 0.2
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def count_mark_workua(self, candidate):
        education = candidate.get("education", [])
        job_experience = candidate.get("job_experience", [])
        skill_stack = candidate.get("skill_stack", [])
        languages = candidate.get("language", [])

        return self.weights_table["education"] * len(education) +\
               self.weights_table["working_experience"] * len(job_experience) +\
               self.weights_table["skills"] * len(skill_stack) +\
               self.weights_table["languages"] * len(languages)

    def count_mark_rabotaua(self, candidate):
        education = candidate.get("education", [])
        job_experience = candidate.get("job_experience", [])
        skill_stack = [s for s in candidate.get("skills", []) if s]
        languages = candidate.get("languages", [])

        return self.weights_table["education"] * len(education) + \
            self.weights_table["working_experience"] * len(job_experience) + \
            self.weights_table["skills"] * len(skill_stack) + \
            self.weights_table["languages"] * len(languages)


def save_parsing_history_to_excel(date_key):
    with DataBaseManager() as db:
        document = db.fetch_data(date_key)
        if not document:
            print(f"No data found for {date_key}")
            return
        names = []
        for resource_name, queries in document.items():
            if resource_name == "_id":
                continue
            for query, data in queries.items():
                df = pd.DataFrame(data)

                filename = f"{resource_name}_{date_key.replace('.', '')}_{query.replace(' ', '_')}.xlsx"

                df.to_excel(filename, index=False)
                print(f"Data for query '{query}' saved to {filename}")
                names.append(filename)
        return names


if __name__ == "__main__":
    with DataBaseManager() as db:
        print()
