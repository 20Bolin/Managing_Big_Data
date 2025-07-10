from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, mean, count, min, max
import matplotlib.pyplot as plt

# Initialize a Spark session
spark = SparkSession.builder.appName("GreenTaxiHVFHVAnalysis").getOrCreate()

# Set the log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Define the base paths for the green taxi and HVFHV data
base_path_green = "/user/s3022455/taxiData/Green/"
base_path_hvfhv = "/user/s3022455/taxiData/HVFHV/"

# Initialize empty DataFrames for the green taxi and HVFHV data by reading the first available dataset
green_taxi_df= spark.read.parquet(f"{base_path_green}2019/*.parquet")
hvfhv_df = spark.read.parquet(f"{base_path_hvfhv}2019/*.parquet")

# Load data from each subsequent year for both datasets and union with the initial Dataframe
for year in range(2019, 2023):  # Up to December 2022
    # Read all Parquet files for the current year for green taxi
    yearly_data_green = spark.read.parquet(f"{base_path_green}2019/*.parquet")
    # Read all Parquet files for the current year for HVFHV
    yearly_data_hvfhv = spark.read.parquet(f"{base_path_hvfhv}2019/*.parquet")

    # Union the yearly data with the main DataFrames
    green_taxi_df = green_taxi_df.unionByName(yearly_data_green, allowMissingColumns=True)
    hvfhv_df = hvfhv_df.unionByName(yearly_data_hvfhv, allowMissingColumns=True)

# Show the first few rows of the green_taxi_df DataFrame
print("Green Taxi DataFrame:")
green_taxi_df.show()

# Show the first few rows of the hvfhv_df DataFrame
print("HVFHV DataFrame:")
hvfhv_df.show()

# Select only the 'trip_distance' and 'trip_miles'  column and convert miles to kilometers if necessary
green_taxi_df = green_taxi_df.select(
    (col("trip_distance") * 1.60934).alias("trip_distance_km")
)

hvfhv_df = hvfhv_df.select(
    (col("trip_miles") * 1.60934).alias("trip_distance_km")
)

# REMOVING OUTLIERS

# Define reasonable limits for trip distances
MIN_DISTANCE = 0  # minimum distance in km
MAX_DISTANCE = 500  # maximum distance in km

# Clean and filter Green Taxi DataFrame
green_taxi_df_clean = green_taxi_df.filter(
    (col("trip_distance_km") > MIN_DISTANCE) &
    (col("trip_distance_km") <= MAX_DISTANCE)
)

# Clean and filter HVFHV DataFrame
hvfhv_df_clean = hvfhv_df.filter(
    (col("trip_miles") > MIN_DISTANCE) &
    (col("trip_miles") <= MAX_DISTANCE)
)

# Define the distance thresholds for categorizing trips
short_distance_threshold = 5  # km
long_distance_threshold = 15  # km

# Create a new categorical column 'trip_type_category' in both DataFrames
green_taxi_df = green_taxi_df_clean.withColumn(
    'trip_type_category',
    when(col('trip_distance_km') < short_distance_threshold, 'short')
    .when(col('trip_distance_km') <= long_distance_threshold, 'medium')
    .otherwise('long')
)

hvfhv_df = hvfhv_df_clean.withColumn(
    'trip_type_category',
    when(col('trip_distance_km') < short_distance_threshold, 'short')
    .when(col('trip_distance_km') <= long_distance_threshold, 'medium')
    .otherwise('long')
)

# Check for null values in trip_distance for green taxis
green_taxi_nulls = green_taxi_df.filter(col("trip_distance").isNull()).count()
print(f"Number of null values in trip_distance for green taxis: {green_taxi_nulls}")

# Remove rows with null values in trip_distance for green taxis
green_taxi_df = green_taxi_df.filter(col("trip_distance").isNotNull())

# Check for null values in trip_distance for HVFHV
hvfhv_nulls = hvfhv_df.filter(col("trip_miles").isNull()).count()
print(f"Number of null values in trip_distance for HVFHV: {hvfhv_nulls}")

# Remove rows with null values in trip_distance for HVFHV
hvfhv_df = hvfhv_df.filter(col("trip_miles").isNotNull())

# DATA SEGMENTATION

# Segment the data based on the trip type categories
green_taxi_short_trips = green_taxi_df.filter(col('trip_type_category') == 'short')
green_taxi_medium_trips = green_taxi_df.filter(col('trip_type_category') == 'medium')
green_taxi_long_trips = green_taxi_df.filter(col('trip_type_category') == 'long')

hvfhv_short_trips = hvfhv_df.filter(col('trip_type_category') == 'short')
hvfhv_medium_trips = hvfhv_df.filter(col('trip_type_category') == 'medium')
hvfhv_long_trips = hvfhv_df.filter(col('trip_type_category') == 'long')

# Show the count for each segment
print("Green Taxi Short Trips Count:", green_taxi_short_trips.count())
print("Green Taxi Medium Trips Count:", green_taxi_medium_trips.count())
print("Green Taxi Long Trips Count:", green_taxi_long_trips.count())
print("HVFHV Short Trips Count:", hvfhv_short_trips.count())
print("HVFHV Medium Trips Count:", hvfhv_medium_trips.count())
print("HVFHV Long Trips Count:", hvfhv_long_trips.count())

# DESCRIPTIVE ANALYSIS

green_taxi_df.show()

# Calculate overall mean and median trip distance for Green Taxis
green_taxi_overall_stats = green_taxi_df.agg(
    F.count("trip_distance_km").alias("total_num_trips"),
    F.mean("trip_distance_km").alias("overall_mean_distance"),
    F.percentile_approx("trip_distance_km", 0.5).alias("overall_median_distance")
)

# Calculate overall mean and median trip distance for HVFHV
hvfhv_overall_stats = hvfhv_df.agg(
    F.count("trip_distance_km").alias("total_num_trips"),
    F.mean("trip_distance_km").alias("overall_mean_distance"),
    F.percentile_approx("trip_distance_km", 0.5).alias("overall_median_distance")
)

# Find the min and max values for the Green Taxi DataFrame
green_min_max = green_taxi_df.agg(
    min("trip_distance_km").alias("min_distance"),
    max("trip_distance_km").alias("max_distance")
).collect()

green_min_distance = green_min_max[0]['min_distance']
green_max_distance = green_min_max[0]['max_distance']

print(f"Green Taxi - Min Distance: {green_min_distance}, Max Distance: {green_max_distance}")

# Find the min and max values for the HVFHV DataFrame
hvfhv_min_max = hvfhv_df.agg(
    min("trip_distance_km").alias("min_distance"),
    max("trip_distance_km").alias("max_distance")
).collect()

hvfhv_min_distance = hvfhv_min_max[0]['min_distance']
hvfhv_max_distance = hvfhv_min_max[0]['max_distance']

print(f"HVFHV - Min distance: {hvfhv_min_distance}, Max distance: {hvfhv_max_distance}")

# Show the overall stats for Green Taxis
print("Overall Green Taxi Stats:")
green_taxi_overall_stats.show()

# Show the overall stats for HVFHV
print("Overall HVFHV Stats:")
hvfhv_overall_stats.show()

# PLOTTING

# get the counts for each category
green_short_count = green_taxi_short_trips.count()
green_medium_count = green_taxi_medium_trips.count()
green_long_count = green_taxi_long_trips.count()

hvfhv_short_count = hvfhv_short_trips.count()
hvfhv_medium_count = hvfhv_medium_trips.count()
hvfhv_long_count = hvfhv_long_trips.count()

# create lists of counts and labels for the pie charts
green_counts = [green_short_count, green_medium_count, green_long_count]
hvfhv_counts = [hvfhv_short_count, hvfhv_medium_count, hvfhv_long_count]
labels = ['Short', 'Medium', 'Long']
colors = ['#ff9999', '#66b3ff', '#99ff99']

# Green Taxi Pie Chart
fig1, ax1 = plt.subplots()
ax1.pie(green_counts, colors=colors, labels=labels, autopct='%.2f%%', startangle=90)
centre_circle = plt.Circle((0, 0), 0.70, fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)
ax1.axis('equal')
plt.tight_layout()
plt.subplots_adjust(top=0.85)
plt.title('Green Taxi - Segmentation of Trips', pad=20)
plt.savefig('/home/s2819201/green_taxi_trip_segmentation.png', bbox_inches='tight', dpi=300)
plt.close(fig)  # Close the figure to free up memory

# HVFHV Pie Chart
fig1, ax1 = plt.subplots()
ax1.pie(hvfhv_counts, colors=colors, labels=labels, autopct='%.2f%%', startangle=90)
centre_circle = plt.Circle((0, 0), 0.70, fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)
ax1.axis('equal')
plt.tight_layout()
plt.subplots_adjust(top=0.85)
plt.title('HVFHV - Segmentation of Trips', pad=20)
plt.savefig('/home/s2819201/hvfhv_trip_segmentation.png', bbox_inches='tight', dpi=300)
plt.close(fig)  # Close the figure to free up memory

# Define a range for sampling
green_min_sample = 0
green_max_sample = 50
HVFHV_min_sample = 0
HVFHV_max_sample = 50

# Sampling 8% of the data for Green Taxi
green_sample = green_taxi_df.filter(
    (col("trip_distance_km") >= green_min_sample) & (col("trip_distance_km") <= green_max_sample)
).sample(False, 0.08).select('trip_distance_km').rdd.flatMap(lambda x: x).collect()

# Plot the histogram for Green Taxi
plt.figure(figsize=(10, 5))
plt.hist(green_sample, bins=50, color='green', alpha=0.7)

# Lines for threshold, mean, and median
threshold_short = 5  # km for short trips
threshold_long = 15  # km for long trips
green_mean_distance = green_taxi_df.select(mean('trip_distance_km')).collect()[0][0]
green_median_distance = green_taxi_df.approxQuantile('trip_distance_km', [0.5], 0.05)[0]

plt.axvline(threshold_short, color='yellow', linestyle='dashed', linewidth=1, label=f'Short Threshold: {threshold_short} km')
plt.axvline(threshold_long, color='red', linestyle='dashed', linewidth=1, label=f'Long Threshold: {threshold_long} km')
plt.axvline(green_mean_distance, color='blue', linestyle='dashed', linewidth=1, label=f'Mean: {green_mean_distance:.2f} km')
plt.axvline(green_median_distance, color='orange', linestyle='dashed', linewidth=1, label=f'Median: {green_median_distance:.2f} >
plt.title('Green Taxi Trip Distance Distribution')
plt.xlabel('Distance (km)')
plt.ylabel('Frequency')
plt.legend()

#Save the figure for Green Taxi
plt.savefig('/home/s2819201/green_taxi_trip_distance_distribution.png')
plt.close()

# Sampling 8% of the data for HVFHV
hvfhv_sample = hvfhv_df.filter(
    (col("trip_distance_km") >= HVFHV_min_sample) & (col("trip_distance_km") <= HVFHV_max_sample)
).sample(False, 0.08).select('trip_distance_km').rdd.flatMap(lambda x: x).collect()

# Plot the histogram for HVFHV
plt.figure(figsize=(10, 5))
plt.hist(hvfhv_sample, bins=50, color='blue', alpha=0.7)

# Lines for threshold, mean, and median
hvfhv_mean_distance = hvfhv_df.select(mean('trip_distance_km')).collect()[0][0]
hvfhv_median_distance = hvfhv_df.approxQuantile('trip_distance_km', [0.5], 0.05)[0]

plt.axvline(threshold_short, color='yellow', linestyle='dashed', linewidth=1, label=f'Short Threshold: {threshold_short} km')
plt.axvline(threshold_long, color='red', linestyle='dashed', linewidth=1, label=f'Long Threshold: {threshold_long} km')
plt.axvline(hvfhv_mean_distance, color='green', linestyle='dashed', linewidth=1, label=f'Mean: {hvfhv_mean_distance:.2f} km')
plt.axvline(hvfhv_median_distance, color='orange', linestyle='dashed', linewidth=1, label=f'Median: {hvfhv_median_distance:.2f} >

# Add title and labels
plt.title('HVFHV Trip Distance Distribution')
plt.xlabel('Distance (km)')
plt.ylabel('Frequency')
plt.legend()

# Save the figure for HVFHV
plt.savefig('/home/s2819201/hvfhv_trip_distance_distribution.png')
plt.close()

# Stop the Spark Session
spark.stop()