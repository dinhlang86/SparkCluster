from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Visa Application Japan").getOrCreate()

df = spark.read.csv('input/visa_number_in_japan.csv',
                    header=True,
                    inferSchema=True)

# Replace spaces, slashes, dots, and commas in column names
new_col_names = [col.replace(' ', '_')
                    .replace('/', '')
                    .replace('.', '')
                    .replace(',', '') for col in df.columns]
df = df.toDF(*new_col_names)

# Drop all columns with null values
df = df.dropna(how='all')

# Get the necessary columns
df = df.select('year', 'country', 'number_of_issued_numerical')

df.show()

spark.stop()
