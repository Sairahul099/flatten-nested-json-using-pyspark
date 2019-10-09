
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,to_date,lit,udf,to_timestamp,to_json,get_json_object,col
from pyspark.sql.types import StringType,TimestampType,LongType,DoubleType
import json

spark = SparkSession \
        .builder \
        .appName("nestedapp") \
        .master("local") \
        .getOrCreate()

#udf to get the value if the datatype is of string
def getStringType(jsonString,jsonPath):
    jsonPathList = jsonPath.split('.')
    jsonPathList.append('endjson')
    rowMap = json.loads(jsonString)
    for i in jsonPathList:
        if(rowMap):
           if(i.startswith('val') and int(i[3:]) < len(rowMap) and type(rowMap) is list):
               rowMap = rowMap[int(i[3:])]
           elif(type(rowMap) is dict):
               rowMap = rowMap.get(i)
           elif(i == "endjson"):
               return str(rowMap)

#udf to get the value if the datatype is of int
#As we dont know whether the type of int in python we are using long type to accomodate all the integer types while  extracting
def getLongType(jsonString,jsonPath):
    jsonPathList = jsonPath.split('.')
    jsonPathList.append('endjson')
    rowMap = json.loads(jsonString)
    for i in jsonPathList:
        if(rowMap):
           if(i.startswith('val') and int(i[3:]) < len(rowMap) and type(rowMap) is list):
               rowMap = rowMap[int(i[3:])]
           elif(type(rowMap) is dict):
               rowMap = rowMap.get(i)
           elif(i == "endjson"):
               return int(rowMap)
            
#udf to get the value if the datatype is of float or double          
def getDoubleType(jsonString,jsonPath):
    jsonPathList = jsonPath.split('.')
    jsonPathList.append('endjson')
    rowMap = json.loads(jsonString)
    for i in jsonPathList:
        if(rowMap):
           if(i.startswith('val') and int(i[3:]) < len(rowMap) and type(rowMap) is list):
               rowMap = rowMap[int(i[3:])]
           elif(type(rowMap) is dict):
               rowMap = rowMap.get(i)
           elif(i == "endjson"):
               return float(rowMap)

#Json function to get all the nested key paths  with dot notation 
def getJsonPath(jsonString):
    rowMap = json.loads(jsonString)
    flattenmap = {}
    def flat(x,name):
        if type(x) is dict:
            for a in x:
                flat(x[a],name + a + '.')
        elif type(x) is list:
            for i in range(len(x)):
                flat(x[i], name + 'val' + str(i) + '.')
        else:
            if(type(x) is int):
                flattenmap[name] = "int"
            elif(type(x) is float):
                flattenmap[name] = "float"
            else:
                flattenmap[name] = "str"

    flat(rowMap,'')
    return json.dumps(flattenmap)






castFields = udf(getJsonPath, StringType())
stringData = udf(getStringType, StringType())
longData = udf(getLongType,LongType())
doubleData = udf(getDoubleType,DoubleType())

#Reading the dataframe as text file
df = spark.read.text('flattest.json')

#Appending the json paths as column  to dataframe
df = df.withColumn('jsonpaths', castFields(col('value')))

schemanames_dict = {}

schema_names_info = df.select('jsonpaths').collect()

for schema_name_row in schema_names_info:
    schema_name_dict = schema_name_row.asDict()
    schema_name_row =  json.loads(schema_name_dict['jsonpaths'])
    for key,value in schema_name_row.items():
        schema_name = key[0:len(key) - 1]
        gettype = schemanames_dict.get(schema_name)
        if(gettype):
            if(schemanames_dict[schema_name] != gettype):
                schemanames_dict[schema_name] = "str"
        else:
            schemanames_dict[schema_name] = value

df = df.drop('jsonpaths')

#Appending all the nested key values as columns to dataframe
for key,value in schemanames_dict.items():
    #print(key,value)
    if(value == "str"):
        df = df.withColumn(key,stringData(col('value'),lit(key)))
    elif(value == "int"):
        df = df.withColumn(key, stringData(col('value'), lit(key)))
    elif(value == "float"):
        df = df.withColumn(key, stringData(col('value'), lit(key)))

df.printSchema()
df.show()
