from pyspark.sql import Window as W
from pyspark.sql.functions import col, sum, when, lit, concat_ws, collect_list


# Define the lift data as a list of tuples, where each tuple represents a lift with its ID and capacity in kg
lift_data = [
    (1, 300),
    (2, 350)
]

# Define the schema for the lift data
lift_schema = "id int, capacity_kg int"

# Create a DataFrame for the lift data using the defined schema
lift_df = spark.createDataFrame(data=lift_data, schema=lift_schema)

# Define the lift passengers data as a list of tuples, where each tuple represents a passenger with their name, weight in kg, and the lift ID they are assigned to
lift_passengers_data = [
    ('Rahul', 85, 1),
    ('Adarsh', 73, 1),
    ('Riti', 95, 1),
    ('Viraj', 80, 1),
    ('Vimal', 83, 2),
    ('Neha', 77, 2),
    ('Priti', 73, 2),
    ('Himanshi', 85, 2)
]

# Define the schema for the lift passengers data
lift_passengers_schema = "passenger_name string, weight_kg int, lift_id int"

# Create a DataFrame for the lift passengers data using the defined schema
passengers_df = spark.createDataFrame(data=lift_passengers_data, schema=lift_passengers_schema)

# Uncomment the following lines to display the DataFrames
# lift_df.show()
# passengers_df.show()


# Define a window specification to partition by 'lift_id' and order by 'weight_kg'
window_spec = W.partitionBy(col("lift_id")).orderBy(col("weight_kg"))

# Perform the operations on the DataFrame
result_df = (
    # Join the passengers DataFrame with the lift DataFrame on 'lift_id'
    passengers_df.join(lift_df, on=passengers_df.lift_id == lift_df.id, how='inner')
    # Drop the 'id' column from the resulting DataFrame
    .drop("id")
    # Add a new column 'Running_sum' which is the cumulative sum of 'weight_kg' within each partition defined by 'window_spec'
    .withColumn("Running_sum", sum(col("weight_kg")).over(window_spec))
    # Add a new column 'Weight_flag' which is '1' if 'Running_sum' is less than or equal to 'capacity_kg', otherwise '0'
    .withColumn("Weight_flag", when(col("Running_sum") <= col("capacity_kg"), lit("1")).otherwise(lit("0")))
    # Filter the DataFrame to include only rows where 'Weight_flag' is '1'
    .filter("Weight_flag = 1")
    # Group by 'lift_id' and aggregate the passenger names into a comma-separated string
    .groupBy(col("lift_id")).agg(concat_ws(',', collect_list(col("passenger_name"))).alias("passengers"))
)

# Display the resulting DataFrame without truncating the output
result_df.show(truncate=False)