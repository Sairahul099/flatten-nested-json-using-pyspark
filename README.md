# Flatten-nested-json-using-pyspark

The following repo is about to unnest all the fields of json and make them as top level dataframe Columns using pyspark in aws glue Job.
When a spark RDD reads a dataframe using json function it identifies the top level keys of json and converts them to dataframe columns.

In this program we are going to read the json lines as text this is because when we fetch the data from different databases different keys have different datatypes and this lead to schema issues while querying in aws athena.if your database does not allow this type of issue we dont need to cast the fields expliciy and we can directly read the json using rdd read jso function  and unnest the top level columns

The following code will work on large json datasets in aws glue jobs.In local pc these will work on small datasets
In the following we read the json files as text and use json.loads function to get all the nested keys with dot notation and 
datatypes of respective fields.If we encounter any array in path we are going to give val and index position while naming nested key
All the above functionalities are achieved using pyspark udf function 

After getting all the nested key values pairs with dot notation and datatypes.if we encounter different datatypes for a single field
we are going to cast them explicitly as string

With use of nested key value paths in dot notation and datatype we are going to get the values using respective datatype udf








