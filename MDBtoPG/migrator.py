class MongoToPostgresMigrator:
    """
    Migrate data from MongoDB to PostgreSQL
    """
    def __init__(self,mongo_client,mongo_db,mongo_co,postgresql_conexion):
        """Constructor
        """
        self.mongo_client=mongo_client
        self.postgresql_conexion=postgresql_conexion
        self.mongo_db=mongo_db
        self.mongo_co=mongo_co

    def migrate(self,columns_mapping=1,varchar_size=50):
        if columns_mapping:
            data_mongo,columns,query=self._mongo_data()
        else:
            data_mongo,columns,query=self._mongo_data(columns_mapping)
        data_types=self._get_dtype(query[0],varchar_size)
        self._create_table(columns,data_types)
        querys = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
        """
        cursor1=self.postgresql_conexion.cursor()
        cursor1.execute(querys)
        for elem in cursor1:
            print(elem)
        cursor1.close()
        insert='('
        for k in range(len(columns)):
            insert+='%s,'
        insert=insert[:-1]+')'
        cursor2=self.postgresql_conexion.cursor()
        print(f'Sample of the data to insert : {data_mongo[0]}')
        print(f'format to insert : {insert}')
        insert_query=f"INSERT INTO public.{self.mongo_co} VALUES {insert}"
        for row in data_mongo:
            cursor2.execute(insert_query,row)
        self.postgresql_conexion.commit()
        self.postgresql_conexion.close()
        print('Data migrated successfully')
         
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
            query=list(collection.find({}))
            documents=[tuple(document.values()) for document in query]
        else:
            query=list(collection.find({},{column:1 for column in columns_mapping}))
            documents=[tuple(document.values()) for document in query]
        for elem in query:
            for key in elem.keys():
                if key not in columns:
                    columns.append(key)
        return documents,columns,query
    
    def _get_dtype(self,data:dict,varchar_size):
        dtype=[]
        for elem in data.values():
            if type(elem) not in dtype:
                if type(elem) is int:
                    dtype.append('INTEGER')
                elif type(elem) is float:
                    dtype.append('FLOAT')
                elif type(elem) is str:
                    dtype.append('VARCHAR('+str(varchar_size)+')')
                elif type(elem) is bool:
                    dtype.append('BOOLEAN')
                elif type(elem) is dict:
                    dtype.append('json')
                else:
                    dtype.append('VARCHAR('+str(varchar_size)+')')
        return dtype
    
    def _create_table(self,columns:list,dtypes:list):
        cursor=self.postgresql_conexion.cursor()
        query=f'CREATE TABLE {self.mongo_co} ('
        for column,dtype in zip(columns,dtypes):
            query+=column+' '+dtype+','
        query=query[:-1]+')'
        cursor.execute(query)    