# Sample data as a list of tuples
data = [
    (1, "Sagar", 23, "Male", 68.0),
    (2, "Kim", 35, "Female", 90.2),
    (3, "Alex", 40, "Male", 79.1),
]

# Schema definition as a string
schema = "Id int,Name string,Age int,Gender string,Marks float"

# Create a DataFrame using the SparkSession's createDataFrame method
# 'data' is the list of tuples and 'schema' is the schema definition
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Initialize an empty dictionary to hold columns grouped by their data types
d = {}

# Iterate over the DataFrame's dtypes attribute, which contains column names and their data types
for col_name, d_type in df.dtypes:
    # Uncomment the following line to print the column name and its data type
    # print(f"column name is {col_name} and data type is {d_type}")

    # Check if the data type is not already a key in the dictionary
    if d_type not in d:
        # If not, initialize an empty list for this data type
        d[d_type] = []

    # Append the column name to the list corresponding to its data type
    d[d_type].append(col_name)

# The dictionary 'd' now contains lists of column names grouped by their data types

# Iterate over the dictionary 'd' which contains data types as keys and lists of column names as values
for key, value in d.items():
    # Uncomment the following lines if you want to use the data type as the table name
    # table_name = str(d[key])
    # df.select(d[key]).write.format("delta").mode('overwrite').saveAsTable(f'{d[key]}')
    
    # Print the current key (data type) and its associated column names
    print(f'key is {key} and values are {value}')
    
    # Select the columns corresponding to the current data type and display the DataFrame
    df.select(value).show()
    
    # Write the selected columns to a Delta table, using the data type as the table name
    df.select(value).write.format("delta").mode("overwrite").saveAsTable(key)
