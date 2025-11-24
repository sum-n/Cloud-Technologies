from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import trim, regexp_replace, initcap, col, sum as _sum, round as spark_round, lag, avg as spark_avg
import matplotlib.pyplot as plt
import pandas as pd
import os

# Starting the Spark session
spark = SparkSession.builder \
    .appName("CircularEconomyAnalysis") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "8") \
    .config("spark.cores.max", "16") \
    .config("spark.default.parallelism", "16") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # Only show errors
print("Connected to Spark cluster")

# Loading in the datasets
print("\nLoading in the datasets")
waste_generated = spark.read.csv("/data/waste.csv", header=True, inferSchema=True)
waste_treated = spark.read.csv("/data/waste_treated.csv", header=True, inferSchema=True)
print("Datasets loaded successfully.")

# Check for missing values in the datasets
print(f"\nWaste Generated dataset has {waste_generated.filter(col('VALUE').isNull()).count()} missing VALUE entries")
print(f"Waste Treated dataset has {waste_treated.filter(col('VALUE').isNull()).count()} missing VALUE entries")

# Cleaning Waste Management Operation names by removing unnecessary brackets and capitalizing words
waste_treated = waste_treated.withColumn(
    "Waste Management Operation", 
    initcap(trim(
        regexp_replace(
            regexp_replace(col("Waste Management Operation"), r"\[.*?\]", ""),
            r"\(.*?\)", ""
        )
    ))
)

print("\nCleaning Waste Management Operation names for clearity")
waste_treated.select("Waste Management Operation").distinct().show(20, truncate=False)

# Remove rows that has missing values
waste_generated = waste_generated.filter(col("VALUE").isNotNull())
waste_treated = waste_treated.filter(col("VALUE").isNotNull())

print("Data Cleaning Completed.")

# First analysis
# Total Waste Generated in million tonnes vs Treated by Year (Converting tonnes to million tonnes for easier readability)
total_generated = waste_generated \
    .filter(col("Hazardousness") == "Hazardous and non-hazardous - Total[HAZ_NHAZ]") \
    .groupBy("Year") \
    .agg(spark_round(_sum("VALUE") / 1000000, 2).alias("Total_Waste_Generated_Million_Tonnes"))

total_treated = waste_treated \
    .filter(col("Hazardousness") == "Hazardous and non-hazardous - Total[HAZ_NHAZ]") \
    .groupBy("Year") \
    .agg(spark_round(_sum("VALUE") / 1000000, 2).alias("Total_Waste_Treated_Million_Tonnes"))

summary_by_year = total_generated.join(total_treated, on="Year", how="left") \
    .withColumn("Percentage_of_Waste_Treated", 
                spark_round((col("Total_Waste_Treated_Million_Tonnes") / \
                    col("Total_Waste_Generated_Million_Tonnes")) * 100, 2)) \
    .orderBy("Year")

summary_by_year.show(50, truncate=False)

# Plotting the Waste Generation vs Waste Treatment with Percentage of Waste Treated trend line
# Convert the already-pivoted and renamed Spark DataFrame to pandas for plotting
summary_data = summary_by_year.toPandas()
fig, ax1 = plt.subplots(figsize=(12, 6))

# Bar chart for waste amounts (left axis)
x = range(len(summary_data))
width = 0.35
ax1.bar([i - width/2 for i in x], summary_data['Total_Waste_Generated_Million_Tonnes'], 
        width, label='Waste Generated', color='#e74c3c', alpha=0.8)
ax1.bar([i + width/2 for i in x], summary_data['Total_Waste_Treated_Million_Tonnes'], 
        width, label='Waste Treated', color='#27ae60', alpha=0.8)

ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
ax1.set_ylabel('Waste Amount (Million Tonnes)', fontsize=12, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(summary_data['Year'])
ax1.legend(loc='upper left')
ax1.grid(True, alpha=0.3, axis='y')

# Line chart for percentage (right axis)
ax2 = ax1.twinx()
ax2.plot(x, summary_data['Percentage_of_Waste_Treated'], 
         color='#3498db', marker='o', linewidth=2.5, markersize=8, 
         label='Treatment Rate (%)')
ax2.set_ylabel('Treatment Rate (%)', fontsize=12, fontweight='bold', color='#3498db')
ax2.tick_params(axis='y', labelcolor='#3498db')
ax2.legend(loc='upper right')

plt.title('Irish Waste Generation vs Treatment (2004-2020)', fontsize=14, fontweight='bold')
fig.tight_layout()
plt.savefig('/data/output/waste_generation_vs_treatment.png', dpi=300, bbox_inches='tight')
plt.close()

# Second Analysis
# All the different ways waste is treated over time
# Group by Year and Waste Management Operation
treatment_methods = waste_treated.groupBy("Year", "Waste Management Operation") \
    .agg(spark_round(_sum("VALUE") / 1000000, 2).alias("Amount_in_Million_Tonnes")) \
    .orderBy("Year", "Waste Management Operation")

# Pivot to show each treatment method as a column
treatment_pivot = waste_treated.groupBy("Year") \
    .pivot("Waste Management Operation") \
    .agg(spark_round(_sum("VALUE") / 1000000, 2)) \
    .orderBy("Year")

# Remove "All Waste treatment" column if exists as it is redundant
if "All Waste Treatment" in treatment_pivot.columns:treatment_pivot = treatment_pivot.drop("All Waste Treatment")

# Remove redundant/overlapping columns - keep only the most comprehensive ones
columns_to_keep = ['Year', 'Disposal - Incineration', 'Disposal - Landfill','Disposal - Landfill and Other', 
                   'Disposal - Other', 'Recovery - Backfilling', 'Recovery - Energy Recovery', 
                   'Recovery - Recycling and Backfilling', 'Recovery - Recycling']
treatment_pivot = treatment_pivot.select([c for c in columns_to_keep if c in treatment_pivot.columns])

# Calculating recycling rate (Recovery / Total)
if "Recovery - Recycling and Backfilling" in treatment_pivot.columns and "Disposal - Landfill and Other" in treatment_pivot.columns:
    treatment_pivot = treatment_pivot.withColumn(
        "Recycling_Rate_Percentage",
        spark_round(
            (col("Recovery - Recycling and Backfilling") / 
             (col("Disposal - Landfill and Other") + 
              col("Recovery - Recycling and Backfilling"))) * 100, 2
        )
    )
       
print("\nTreatment methods by year:")
treatment_pivot.show(50, truncate=False)

# Plotting stacked bar chart for treatment methods over the years
treatment_data_pivot = treatment_pivot.toPandas()

# Setting Year as index column and exclude the Recycling_Rate_Percentage column
treatment_data_pivot = treatment_data_pivot.set_index('Year')
if 'Recycling_Rate_Percentage' in treatment_data_pivot.columns:
    treatment_data_pivot = treatment_data_pivot.drop('Recycling_Rate_Percentage', axis=1)

# Fill any NaN values with 0 (in case some treatment methods are missing in certain years)
treatment_data_pivot = treatment_data_pivot.fillna(0)

# Plot
treatment_data_pivot.plot(kind='bar', stacked=True, figsize=(12, 7))
plt.title('Waste Treatment Methods (2004-2020)')
plt.xlabel('Year')
plt.ylabel('Amount (Million Tonnes)')
plt.legend(title='Waste Management Operation', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig('/data/output/waste_treatment_methods.png')
plt.close()

# Third Analysis
# Hazardous vs Non-Hazardous Waste Trends
hazard_analysis = waste_generated.groupBy("Year", "Hazardousness") \
    .agg(spark_round(_sum("VALUE") / 1000000, 2).alias("Amount_in_Million_Tonnes")).orderBy("Year", "Hazardousness")

hazard_pivot = waste_generated.groupBy("Year").pivot("Hazardousness") \
    .agg(spark_round(_sum("VALUE") / 1000000, 2)).orderBy("Year")

# Plot amount of hazardous vs non-hazardous waste over the years
hazard_data = hazard_analysis.toPandas()
plt.figure(figsize=(10, 6))
for hazard_type in hazard_data['Hazardousness'].unique():
    subset = hazard_data[hazard_data['Hazardousness'] == hazard_type]
    plt.plot(subset['Year'], subset['Amount_in_Million_Tonnes'], marker='o', label=hazard_type)
plt.title('Hazardous vs Non-Hazardous Waste (2004-2020)')
plt.xlabel('Year')
plt.ylabel('Amount (Million Tonnes)')
plt.legend()
plt.grid(True)
plt.savefig('/data/output/hazardous_vs_non_hazardous_trends.png')

print("\nHazardous vs Non-Hazardous by year in million tonnes:")
hazard_pivot.show(50, truncate=False)

# Fourth Analysis
# Time Series Trends with Moving Averages and Growth Rates
# Year-over-Year Growth Rate
window_year = Window.orderBy("Year")
waste_trends = summary_by_year.withColumn("YoY_Generation_Growth_percentage",
    spark_round(
        ((col("Total_Waste_Generated_Million_Tonnes") - 
          lag("Total_Waste_Generated_Million_Tonnes").over(window_year)) /
         lag("Total_Waste_Generated_Million_Tonnes").over(window_year)) * 100, 2
    )
).withColumn("YoY_Treatment_Growth_percentage",
    spark_round(
        ((col("Total_Waste_Treated_Million_Tonnes") - 
          lag("Total_Waste_Treated_Million_Tonnes").over(window_year)) /
         lag("Total_Waste_Treated_Million_Tonnes").over(window_year)) * 100, 2
    )
)

# 3-Year Moving Average for smoothing trends(technically it's five-point moving average but since we have biannual dates it will cover three years)
window_3year = Window.orderBy("Year").rowsBetween(-1, 1)
waste_trends = waste_trends.withColumn("Generation_3Yr_Moving_Avg", 
                spark_round(spark_avg("Total_Waste_Generated_Million_Tonnes").over(window_3year), 2)
            ).withColumn("Treatment_3Yr_Moving_Avg", 
                spark_round(spark_avg("Total_Waste_Treated_Million_Tonnes").over(window_3year), 2)
        )

print("Time Series Trends with Year over Year Growth and Moving Averages:")
waste_trends.show(50, truncate=False)

# plotting the trends with moving averages
waste_trends_data = waste_trends.toPandas()
plt.figure(figsize=(12, 6))
plt.plot(waste_trends_data['Year'], waste_trends_data['Total_Waste_Generated_Million_Tonnes'], 
         label='Waste Generated', marker='o')
plt.plot(waste_trends_data['Year'], waste_trends_data['Generation_3Yr_Moving_Avg'], 
         label='Generated 3-Year MA', linestyle='--')
plt.plot(waste_trends_data['Year'], waste_trends_data['Total_Waste_Treated_Million_Tonnes'], 
         label='Waste Treated', marker='o')
plt.plot(waste_trends_data['Year'], waste_trends_data['Treatment_3Yr_Moving_Avg'], 
         label='Treated 3-Year MA', linestyle='--')
plt.title('Waste Generation and Treatment Trends with Moving Averages')
plt.xlabel('Year')
plt.ylabel('Amount (Million Tonnes)')
plt.legend()
plt.grid(True)
plt.savefig('/data/output/waste_trends_moving_averages.png')

# Fifth Analysis
# Identify which treatment methods are gaining/losing share over time
treatment_share = waste_treated \
    .filter(col("Hazardousness") == "Hazardous and non-hazardous - Total[HAZ_NHAZ]") \
    .filter(~col("Waste Management Operation").contains("All Waste Treatment")) \
    .groupBy("Year", "Waste Management Operation") \
    .agg(_sum("VALUE").alias("Amount"))

# Calculate total per year
total_per_year = treatment_share.groupBy("Year").agg(_sum("Amount").alias("Total_Amount"))

# Join and calculate percentage share
treatment_share_pct = treatment_share.join(total_per_year, on="Year") \
    .withColumn(
        "Share_Pct", spark_round((col("Amount") / col("Total_Amount")) * 100, 2)
    ).select("Year", "Waste Management Operation", "Share_Pct")

# Calculate trend in share (is recycling increasing? is landfill decreasing? etc.)
window_treatment = Window.partitionBy("Waste Management Operation").orderBy("Year")
treatment_trends = treatment_share_pct.withColumn(
    "Share_Change", spark_round(col("Share_Pct") - lag("Share_Pct").over(window_treatment), 2)
)

print("Treatment Method Market Share Evolution:")
treatment_trends.orderBy("Year", col("Share_Pct").desc()).show(100, truncate=False)

print("\nWaste Analysis Pipeline Completed Successfully")

# Save all the outputs to /data/output
print("\nSaving results to /data/output directory")
os.makedirs("/data/output", exist_ok=True)

# Save all the analysis
summary_by_year.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("/data/output/1_waste_generation_vs_treatment")\

treatment_pivot.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("/data/output/2_waste_treatment_methods")

hazard_pivot.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("/data/output/3_hazardous_analysis")
    
waste_trends.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("/data/output/4_time_series_trends")
    
treatment_trends.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("/data/output/5_treatment_method_evolution")
    
print("\nOutput saved to:")
print("/data/output/1_waste_generation_vs_treatment/")
print("/data/output/waste_generation_vs_treatment.png")
print("/data/output/2_waste_treatment_methods/")
print("/data/output/waste_treatment_methods.png")
print("/data/output/3_hazardous_analysis/")
print("/data/output/hazardous_vs_non_hazardous_trends.png")
print("/data/output/4_time_series_trends/")
print("/data/output/waste_trends_moving_averages.png")
print("/data/output/5_treatment_method_evolution/")

# Stopping the Spark session
spark.stop()
print("\nSpark session stopped. Pipeline completed.")