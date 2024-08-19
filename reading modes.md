In PySpark, when reading CSV files, you can specify different modes to handle corrupt or malformed records. 
Here are the three main read modes:

Permissive (default): This mode sets all fields to null for corrupt records and places the malformed row’s values in a string column called _corrupt_record. You can change the name of this column by setting the spark.sql.columnNameOfCorruptRecord configuration1.

DropMalformed: This mode drops all rows containing corrupt records1.

FailFast: This mode raises an exception and terminates the reading process immediately when corrupt records are encountered1.
Here’s an example of how to specify a read mode when reading a CSV file:


here is the example

empno,ename,job,mgr,hiredate,sal,comm,deptno
7839,KING,PRESIDENT,,1981-11-17,5000,,10
7698,BLAKE,MANAGER,7839,1981-05-01,2850,,30
7782,CLARK,MANAGER,7839,1981-06-09,2450,,10
7566,JONES,MANAGER,7839,1981-04-02,2975,,20
7788,SCOTT,ANALYST,7566,1987-07-13,3000,,20
7902,FORD,ANALYST,7566,1981-12-03,3000,,20
7369,SMITH,CLERK,7902,1980-12-17,800,,20
7499,ALLEN,SALESMAN,7698,1981-02-20,1600,300,30
7521,WARD,SALESMAN,7698,1981-02-22,1250,500,30
7654,MARTIN,SALESMAN,7698,1981-09-28,1250,1400,30
INVALID,DATA,WITH,ERRORS


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("HandleCorruptRecords").getOrCreate()

# Define schema for the EMP table
schema = """
    empno STRING,
    ename STRING,
    job STRING,
    mgr STRING,
    hiredate STRING,
    sal STRING,
    comm STRING,
    deptno STRING
"""

# Read the CSV file with the option to capture corrupt records
df = spark.read.schema(schema)\
    .option("header", "true")\
    .option("mode", "PERMISSIVE")\
    .option("columnNameOfCorruptRecord", "_corrupt_record")\
    .csv("path/to/your/emp_data.csv")

# Show the DataFrame including corrupt records
df.show(truncate=False)

# Filter and display only the corrupt records
corrupt_records_df = df.filter(col("_corrupt_record").isNotNull())
corrupt_records_df.show(truncate=False)
