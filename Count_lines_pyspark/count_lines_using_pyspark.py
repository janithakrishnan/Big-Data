from pyspark import SparkContext

# Define mapper function
def map_to_one(line):
    return 1

# Define reducer function
def add_values(a, b):
    return a + b

# Initialize SparkContext
sc = SparkContext("local", "F:\Python Programs\BigDATA\Count_Lines\sample.txt")

# Load the text file
lines = sc.textFile("sample.txt")

# Apply map and reduce
mapped_lines = lines.map(map_to_one)         # Map step: emit 1 for each line
line_count = mapped_lines.reduce(add_values) # Reduce step: sum all 1s

# Print the result
print("Total number of lines:", line_count)

# Stop SparkContext
sc.stop()