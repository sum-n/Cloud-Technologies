Connected to Spark cluster

Loading in the datasets
Datasets loaded successfully.

Waste Generated dataset has 11233 missing VALUE entries
Waste Treated dataset has 5558 missing VALUE entries

Cleaning Waste Management Operation names for clearity
+------------------------------------+
|Waste Management Operation          |
+------------------------------------+
|Recovery - Recycling And Backfilling|
|Disposal - Landfill And Other       |
|All Waste Treatment                 |
|Disposal - Incineration             |
|Disposal - Landfill                 |
|Disposal - Other                    |
|Recovery - Backfilling              |
|Recovery - Recycling                |
|Recovery - Energy Recovery          |
+------------------------------------+

Data Cleaning Completed.
+----+------------------------------------+----------------------------------+---------------------------+
|Year|Total_Waste_Generated_Million_Tonnes|Total_Waste_Treated_Million_Tonnes|Percentage_of_Waste_Treated|
+----+------------------------------------+----------------------------------+---------------------------+
|2004|194.29                              |93.51                             |48.13                      |
|2006|238.56                              |113.07                            |47.4                       |
|2008|173.62                              |83.23                             |47.94                      |
|2010|168.74                              |65.81                             |39.0                       |
|2012|114.72                              |55.89                             |48.72                      |
|2014|130.52                              |66.83                             |51.2                       |
|2016|134.84                              |78.27                             |58.05                      |
|2018|117.58                              |80.29                             |68.29                      |
|2020|132.8                               |90.7                              |68.3                       |
+----+------------------------------------+----------------------------------+---------------------------+


Treatment methods by year:
+----+-----------------------+-------------------+----------------+----------------------+--------------------------+--------------------+
|Year|Disposal - Incineration|Disposal - Landfill|Disposal - Other|Recovery - Backfilling|Recovery - Energy Recovery|Recovery - Recycling|
+----+-----------------------+-------------------+----------------+----------------------+--------------------------+--------------------+
|2004|0.15                   |29.31              |0.0             |NULL                  |0.67                      |NULL                |
|2006|0.14                   |32.36              |0.09            |NULL                  |0.57                      |NULL                |
|2008|0.08                   |26.56              |0.17            |NULL                  |0.41                      |NULL                |
|2010|0.25                   |18.96              |8.32            |8.87                  |1.01                      |6.88                |
|2012|0.08                   |14.38              |8.0             |8.68                  |2.42                      |4.53                |
|2014|0.08                   |10.97              |7.86            |15.66                 |4.33                      |7.13                |
|2016|0.04                   |14.03              |6.65            |22.24                 |3.35                      |7.0                 |
|2018|0.05                   |9.83               |5.1             |25.82                 |7.24                      |7.91                |
|2020|0.06                   |13.09              |5.11            |30.23                 |7.08                      |7.28                |
+----+-----------------------+-------------------+----------------+----------------------+--------------------------+--------------------+


Hazardous vs Non-Hazardous by year in million tonnes:
+----+---------------------------------------------+--------------+-------------------+
|Year|Hazardous and non-hazardous - Total[HAZ_NHAZ]|Hazardous[HAZ]|Non-hazardous[NHAZ]|
+----+---------------------------------------------+--------------+-------------------+
|2004|194.29                                       |4.2           |190.09             |
|2006|238.56                                       |4.81          |233.74             |
|2008|173.62                                       |4.68          |170.17             |
|2010|168.74                                       |18.92         |149.81             |
|2012|114.72                                       |4.76          |109.95             |
|2014|130.52                                       |4.74          |125.79             |
|2016|134.84                                       |5.49          |129.34             |
|2018|117.58                                       |6.49          |111.09             |
|2020|132.8                                        |9.22          |123.58             |
+----+---------------------------------------------+--------------+-------------------+

Time Series Trends with Year over Year Growth and Moving Averages:
+----+------------------------------------+----------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------------+------------------------+
|Year|Total_Waste_Generated_Million_Tonnes|Total_Waste_Treated_Million_Tonnes|Percentage_of_Waste_Treated|YoY_Generation_Growth_percentage|YoY_Treatment_Growth_percentage|Generation_3Yr_Moving_Avg|Treatment_3Yr_Moving_Avg|
+----+------------------------------------+----------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------------+------------------------+
|2004|194.29                              |93.51                             |48.13                      |NULL                            |NULL                           |216.43                   |103.29                  |
|2006|238.56                              |113.07                            |47.4                       |22.79                           |20.92                          |202.16                   |96.6                    |
|2008|173.62                              |83.23                             |47.94                      |-27.22                          |-26.39                         |193.64                   |87.37                   |
|2010|168.74                              |65.81                             |39.0                       |-2.81                           |-20.93                         |152.36                   |68.31                   |
|2012|114.72                              |55.89                             |48.72                      |-32.01                          |-15.07                         |137.99                   |62.84                   |
|2014|130.52                              |66.83                             |51.2                       |13.77                           |19.57                          |126.69                   |67.0                    |
|2016|134.84                              |78.27                             |58.05                      |3.31                            |17.12                          |127.65                   |75.13                   |
|2018|117.58                              |80.29                             |68.29                      |-12.8                           |2.58                           |128.41                   |83.09                   |
|2020|132.8                               |90.7                              |68.3                       |12.94                           |12.97                          |125.19                   |85.5                    |
+----+------------------------------------+----------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------------+------------------------+

Treatment Method Market Share Evolution:
+----+------------------------------------+---------+------------+
|Year|Waste Management Operation          |Share_Pct|Share_Change|
+----+------------------------------------+---------+------------+
|2004|Recovery - Recycling And Backfilling|45.04    |NULL        |
|2004|Disposal - Landfill                 |27.1     |NULL        |
|2004|Disposal - Landfill And Other       |27.1     |NULL        |
|2004|Recovery - Energy Recovery          |0.62     |NULL        |
|2004|Disposal - Incineration             |0.14     |NULL        |
|2004|Disposal - Other                    |0.0      |NULL        |
|2006|Recovery - Recycling And Backfilling|49.26    |4.22        |
|2006|Disposal - Landfill And Other       |25.1     |-2.0        |
|2006|Disposal - Landfill                 |25.03    |-2.07       |
|2006|Recovery - Energy Recovery          |0.44     |-0.18       |
|2006|Disposal - Incineration             |0.11     |-0.03       |
|2006|Disposal - Other                    |0.07     |0.07        |
|2008|Recovery - Recycling And Backfilling|44.13    |-5.13       |
|2008|Disposal - Landfill And Other       |27.68    |2.58        |
|2008|Disposal - Landfill                 |27.5     |2.47        |
|2008|Recovery - Energy Recovery          |0.43     |-0.01       |
|2008|Disposal - Other                    |0.18     |0.11        |
|2008|Disposal - Incineration             |0.09     |-0.02       |
|2010|Disposal - Landfill And Other       |31.24    |3.56        |
|2010|Disposal - Landfill                 |21.71    |-5.79       |
|2010|Recovery - Recycling And Backfilling|18.04    |-26.09      |
|2010|Recovery - Backfilling              |10.16    |NULL        |
|2010|Disposal - Other                    |9.52     |9.34        |
|2010|Recovery - Recycling                |7.88     |NULL        |
|2010|Recovery - Energy Recovery          |1.15     |0.72        |
|2010|Disposal - Incineration             |0.29     |0.2         |
|2012|Disposal - Landfill And Other       |30.38    |-0.86       |
|2012|Disposal - Landfill                 |19.52    |-2.19       |
|2012|Recovery - Recycling And Backfilling|17.93    |-0.11       |
|2012|Recovery - Backfilling              |11.78    |1.62        |
|2012|Disposal - Other                    |10.86    |1.34        |
|2012|Recovery - Recycling                |6.15     |-1.73       |
|2012|Recovery - Energy Recovery          |3.28     |2.13        |
|2012|Disposal - Incineration             |0.11     |-0.18       |
|2014|Recovery - Recycling And Backfilling|26.0     |8.07        |
|2014|Disposal - Landfill And Other       |21.49    |-8.89       |
|2014|Recovery - Backfilling              |17.87    |6.09        |
|2014|Disposal - Landfill                 |12.52    |-7.0        |
|2014|Disposal - Other                    |8.97     |-1.89       |
|2014|Recovery - Recycling                |8.13     |1.98        |
|2014|Recovery - Energy Recovery          |4.93     |1.65        |
|2014|Disposal - Incineration             |0.09     |-0.02       |
|2016|Recovery - Recycling And Backfilling|28.32    |2.32        |
|2016|Recovery - Backfilling              |21.54    |3.67        |
|2016|Disposal - Landfill And Other       |20.03    |-1.46       |
|2016|Disposal - Landfill                 |13.59    |1.07        |
|2016|Recovery - Recycling                |6.78     |-1.35       |
|2016|Disposal - Other                    |6.44     |-2.53       |
|2016|Recovery - Energy Recovery          |3.25     |-1.68       |
|2016|Disposal - Incineration             |0.04     |-0.05       |
|2018|Recovery - Recycling And Backfilling|32.24    |3.92        |
|2018|Recovery - Backfilling              |24.67    |3.13        |
|2018|Disposal - Landfill And Other       |14.28    |-5.75       |
|2018|Disposal - Landfill                 |9.4      |-4.19       |
|2018|Recovery - Recycling                |7.56     |0.78        |
|2018|Recovery - Energy Recovery          |6.92     |3.67        |
|2018|Disposal - Other                    |4.88     |-1.56       |
|2018|Disposal - Incineration             |0.05     |0.01        |
|2020|Recovery - Recycling And Backfilling|31.64    |-0.6        |
|2020|Recovery - Backfilling              |25.5     |0.83        |
|2020|Disposal - Landfill And Other       |15.35    |1.07        |
|2020|Disposal - Landfill                 |11.04    |1.64        |
|2020|Recovery - Recycling                |6.14     |-1.42       |
|2020|Recovery - Energy Recovery          |5.97     |-0.95       |
|2020|Disposal - Other                    |4.31     |-0.57       |
|2020|Disposal - Incineration             |0.05     |0.0         |
+----+------------------------------------+---------+------------+


Waste Analysis Pipeline Completed Successfully

Saving results to /data/output directory

Output saved to:
/data/output/1_waste_generation_vs_treatment/
/data/output/waste_generation_vs_treatment.png
/data/output/2_waste_treatment_methods/
/data/output/waste_treatment_methods.png
/data/output/3_hazardous_analysis/
/data/output/hazardous_vs_non_hazardous_trends.png
/data/output/4_time_series_trends/
/data/output/waste_trends_moving_averages.png
/data/output/5_treatment_method_evolution/

Spark session stopped. Pipeline completed.