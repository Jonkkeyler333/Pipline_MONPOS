import json

class MongoToPostgresMigrator:
    """
    Migrate data from MongoDB to PostgreSQL
    """
    def __init__(self,mongo_client,mongo_db,mongo_co,postgresql_conexion):
        """Constructor
        """
        self.mongo_client=mongo_client
        self.cursor=postgresql_conexion.cursor()
        self.mongo_db=mongo_db
        self.mongo_co=mongo_co

    def migrate(self,columns_mapping=1):
        if columns_mapping:
            data_mongo,columns=self._mongo_data()
        else:
            data_mongo,columns=self._mongo_data(columns_mapping)
            
    def _get_collection(self):
        """get the collection from the mongoDB

        :return: collection
        :rtype: pymongo.collection.Collection
        """
        db=self.mongo_client[self.mongo_db]
        collection=db[self.mongo_co]
        return collection
    
    def _mongo_data(self,columns_mapping=1):
        """get data from mongoDB

        :param columns_mapping: names of the atributes of the objects to mapping, defaults to 1
        :type columns_mapping: int, optional
        :return: all the documents with the columns names
        :rtype: list, list
        """
        columns=[]
        self.mongo_client
        collection=self._get_collection()
        if columns_mapping:
            documents=list(collection.find({}))
        else:
            documents=list(collection.find({},{column:1 for column in columns_mapping}))
        for elem in documents:
            for key in elem.keys():
                if key not in columns:
                    columns.append(key)
        return documents,columns
    
    def _get_dtype():
        pass
        
    def _create_tables(self,columns):
        pass