import json
import psycopg2
import boto3
import os
from datetime import datetime, timedelta
from io import StringIO
import csv
import json
import re
import pandas
import numpy
import pytz
from psycopg2 import connect, sql




# Global variables for connection to the boto client of AWS Redshift
dbname='{-dbname-}'
host = '{-host-}.ap-south-1.redshift.amazonaws.com'
user = '{-username-}'
password = '{-password-}'
port = '{-port-}'
bucket = '{bucket-name}'



# The bucket directory where the snapshots from the database are to be stored and to be invoked by AWS lambda function for the data to be warehoused
S3_PREFIX = 'Bucket_Dir1/Bucket_Dir2'




# map for the attributes of the enitity tables that are numeric in value
entityIntegerField = {
        'entity_A' : ['NUMERIC_COLUMN_A','NUMERIC_COLUMN_B'] ,
        'entity_B' : ['NUMERIC_COLUMN_A'] ,
        'entity_C' : [],
        'entity_D' : ['NUMERIC_COLUMN_A'],
        'entity_E' : [],
        'entity_F' : ['NUMERIC_COLUMN_A'] ,
        'entity_G' : [] ,
        'entity_H' : [],
    
    }




# defining the lambda handler function or the main function
print("Step 1")   
def lambda_handler(event, context):
    


    csv.field_size_limit(100000000)
    
    print("HELLO")
    
    valx = csv.field_size_limit()
    
    print(valx)
    
    current_date = datetime.utcnow()
    yesterday_utc_date = current_date - timedelta(days=1)
    yesterday_utc_date = yesterday_utc_date.date().strftime('%Y-%m-%d')

    print(yesterday_utc_date)
    print("Pre step ")
    s3 = boto3.client('s3')
    print("Post STEP BOTO")


    # Entity table list
    entities = ['entity_A', 'entity_B', 'entity_C', 'entity_D','entity_E','entity_F', 'entity_G']


    
    # entity table and the attributes map
    entityToFieldsDictionary = {
        'entity_A' : [] ,
        'entity_B' : [] ,
        'entity_C' : [],
        'entity_D' : [],
        'entity_E' : [],
        'entity_F' : [] ,
        'entity_G' : [] ,
        'entity_H' : [],
    }
    
    
    
        
    # looping through the enitity csv that is saved on AWS S3
    for entity in entities:
        tableName = f"""spd_{entity}"""
        tableName = tableName.lower()
        entityFieldsList = entityToFieldsDictionary[entity]
        
        s3FilePath = f"""{S3_PREFIX}/{yesterday_utc_date}/{entity}_{yesterday_utc_date}.csv"""
        
        print(s3FilePath)


        response = s3.get_object(Bucket=bucket, Key=s3FilePath)
        file_content = response['Body'].read().decode('utf-8')
        data = StringIO(file_content)
        jsonReader = csv.DictReader(data)
        entityJsonList = []

        # jsonReader saves the response from the csv object that is stored on AWS S3
        
        
        for row in jsonReader:
            json_obj = json.dumps(row)
            entityJsonList.append(json.loads(json_obj))

    


        # entityJsonList keeps all the data in a list of dictionary form
        
        if(len(entityJsonList) == 0):
            continue
        
        connection = psycopg2.connect(dbname= dbname, host=host, port=port, user=user, password=password)
        
        currentEntityJson = None
        try :
            with connection.cursor() as cur:
                entityIdList = []
                for entityJson in entityJsonList:
                    entityId = entityJson["_id"]   # '_id' can be replaced from the unique id that defines the data
                    entityIdList.append(entityId)
                entityIdSet = set()

                # logging incoming entries from an enitity from the csv
                print(f"""Total Incoming Entries for {tableName} : {len(entityIdList)}""")
                if(len(entityIdList) > 0):
                    selectInQuery = createSelectInQuery(tableName, '_id', entityIdList)
                    cur.execute(selectInQuery)
                    entityIds = cur.fetchall()
                    for entityIdTuple in entityIds:
                        if(len(entityIdTuple) > 0):
                            entityIdSet.add(entityIdTuple[0])

                # logging existing entries from an entity from the csv
                print(f"""Total Existing Entries for {tableName} : {len(entityIdSet)}""")
                newEntitiesJsonList = []
                existingEntitiesJsonList = []
                for entityJson in entityJsonList:
                    id = entityJson["_id"]
                    if id not in entityIdSet:
                        newEntitiesJsonList.append(entityJson)
                    else:
                        existingEntitiesJsonList.append(entityJson)

                # logging update and insert data
                print(f"""Insert Entries length for {tableName} : {len(newEntitiesJsonList)}""")
                print(f"""Update Entries length for {tableName} : {len(existingEntitiesJsonList)}""")
                
                if(len(newEntitiesJsonList) > 0):
                    bulkInsertQuery = prepareBatchInsertQueries(tableName, entityFieldsList, newEntitiesJsonList, entity)
                    cur.execute(bulkInsertQuery)



                # creating temporary tables in case of update query
                if(len(existingEntitiesJsonList) > 0):
                    tempTableName = f"""temporary_{tableName}"""
                    createTabelQuery = f"""CREATE TABLE {tempTableName} (LIKE {tableName}); """
                    cur.execute(createTabelQuery)
                    bulkInsertQuery = prepareBatchInsertQueries(tempTableName, entityFieldsList, existingEntitiesJsonList, entity)
                    cur.execute(bulkInsertQuery)
                    bulkUpdateQuery = prepareBulkUpdateQueries(tableName, tempTableName, entityFieldsList, "_id")
                    cur.execute(bulkUpdateQuery)
                    dropTableQuery = f"""DROP TABLE {tempTableName};"""
                    cur.execute(dropTableQuery)
                    
            connection.commit()


        # keeping it transactional in case of any error rolls back
        except Exception as e:
            print(f"Error: ")
            print(currentEntityJson)
            print(e)
            connection.rollback()
        finally:
            connection.close()

    # after sucessfull execution logging
    print("yayy")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


    




# Defining key functions required for the proper functioning



# numeric field flag function
def isNumberField(field, entity):
    numericFields = entityIntegerField[entity]
    if field in numericFields:
        return True
    else:
        return False

    
def replace_last(string, old, new):
    return new.join(string.rsplit(old, 1))


# select query to get existing entries from redshift
def createSelectInQuery(tableName, field, fieldValues):
    selectInQuery = f"""SELECT {field} FROM {tableName} WHERE {field} IN """
    valueString = "("
    for value in fieldValues:
        valueString = valueString + f""" '{value}' """ + ", "
    valueString = replace_last(valueString, ", ", "")
    valueString = valueString + ")"
    selectInQuery = selectInQuery + valueString
    return selectInQuery
    

# bulk insert query for inserting new data from the csv
def prepareBatchInsertQueries(tableName, fieldList, entityJsonList, entity):
    bulkInsertQuery = f"""INSERT INTO {tableName} """
    fieldString = "("
    for field in fieldList:
        fieldString = fieldString + field + ", "
    fieldString = replace_last(fieldString, ", ", "")
    fieldString = fieldString + ")"
    valueStringList = []
    for entityJson in entityJsonList:
        valueString = "("
        for field in fieldList:
            if field not in entityJson or not entityJson[field]:
                valueString = valueString + f""" NULL """ + ", "
                continue
            if isNumberField(field, entity) and not entityJson[field]:
                valueString = valueString + f""" NULL """ + ", "
            else:
                entityJson[field] = entityJson[field].replace("'", "''") if type(entityJson[field]) == str else entityJson[field]
                valueString = valueString + f""" '{entityJson[field]}' """ + ", "
        
        valueString = replace_last(valueString, ", ", "")
        valueString = valueString + ")"
        valueStringList.append(valueString)
    completeValueString = ', '.join(valueStringList)
    bulkInsertQuery = bulkInsertQuery + f"""{fieldString} VALUES {completeValueString}; """
    
    return bulkInsertQuery



# Bulk update query to update existing entries that are also present in the new csv in case of any change to enitity row 
def prepareBulkUpdateQueries(tableName, tempTableName, entityFieldsList, joinField):
    bulkUpdateQuery = f"""UPDATE {tableName} SET """
    fieldMappingString = ""
    for field in entityFieldsList:
        fieldMappingString = fieldMappingString + f"""{field} = {tempTableName}.{field}""" + ", "
    fieldMappingString = replace_last(fieldMappingString, ", ", "")
    bulkUpdateQuery = bulkUpdateQuery + f"""{fieldMappingString} FROM {tempTableName} WHERE {tableName}.{joinField} = {tempTableName}.{joinField};"""
    
    return bulkUpdateQuery
    
    
    


    

# can be used to list of data from the csv not used in the main code
def get_csv_data_from_s3(bucket, key):
    print("Retrieving CSV data from S3")
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        data = StringIO(file_content)
        reader = csv.DictReader(data)
        return [row for row in reader]
    except:
        return []
    

