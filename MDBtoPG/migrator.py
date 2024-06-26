import json

class MongoToPostgresMigrator:
    """
    Migrate data from MongoDB to PostgreSQL
    """
    def __init__(self,mongo_client,postgresql_client):
        self.mongo_client = mongo_client
        self.postgresql_client = postgresql_client