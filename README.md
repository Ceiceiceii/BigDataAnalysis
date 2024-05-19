## Overview
This README document serves as a comprehensive guide to the directory structure, file descriptions, and operational instructions for our analytics project, which focuses on analyzing NYPD arrest patterns from 2006 to 2024. The project utilizes advanced data processing techniques to uncover insights that can inform law enforcement policies and strategies.

### Project Summary
Our research investigates arrest trends in New York City, utilizing extensive NYPD arrest data available through NYC Open Data. By applying data analytics and machine learning methods, such as k-means clustering, we aim to identify patterns and changes in arrest activities, particularly noting the impacts of societal shifts such as the COVID-19 pandemic. This analysis is intended to provide a nuanced understanding of the dynamics within urban law enforcement and propose data-driven recommendations for policy and operational improvements.

### Directory Structure
- **Analysis/**: Contains Scala scripts for detailed data analysis.
- **Ingestion/**: Scripts for initial data ingestion.
- **etl/**: Includes code for data cleaning, organized into subdirectories for each team member.
- **profiling/**: Holds scripts for data profiling, with directories allocated per contributor.
- **Screenshots/**: All screenshots are stored here for reference.

### File Descriptions
#### Analytics
- `Analysis/2023Analysis.scala`: Analyzes NYPD Arrests Data (Year to Date) for 2023 and compares with previous years.
- `Analysis/analysis2.scala`: Conducts a 15-year cross-analysis to examine arrest pattern variations.
- `Analysis/clustering.scala`: Applies clustering algorithms to discern patterns and groupings in the data.

*Note: All analytics scripts handle data ingestion, ETL/cleaning, and profiling, designed to run in Spark and display results directly in the Spark console.*

#### ETL Code
- `etl/cleanData.scala`: Script for thorough data cleaning, accessible in each team member's subdirectory.

#### Data Ingestion
- `Ingestion/Ingestion.scala`: Filters 2021 data to address changes like the resumption of in-person events post-COVID-19.

#### Profiling Code
- `profiling/profileHist.scala`: Profiles historical data for trend analysis.

#### HDFS Files
- `proj/arrest.csv`: Raw historical record of NYC arrests.
- `proj/cleanedData.csv`: Cleaned historical arrest data.
- `proj/ArrestData.csv`: Raw recent year arrest data.
- `proj/cleanedArrestData.csv`: Cleaned recent year arrest data.

### Instructions for Running the Code
#### Data Cleaning
```bash
spark-shell --deploy-mode client -i etl/cleanData.scala
```

#### Data Profiling
Recent data profiling:
```bash
spark-shell --deploy-mode client -i profiling/profiling.scala
```
Historical data profiling:
```bash
spark-shell --deploy-mode client -i profiling/histProfile.scala
```

#### Data Analysis
2023 arrest data analysis:
```bash
spark-shell --deploy-mode client -i Analysis/2023Analysis.scala
```
Historical arrest data analysis:
```bash
spark-shell --deploy-mode client -i Analysis/analysis2.scala
```
Clustering analysis:
```bash
spark-shell --deploy-mode client -i Analysis/clustering.scala
```

### Additional Notes
This README is crafted to enhance understanding of the project's structure and operations, supporting users in navigating and leveraging the available resources for effective analysis.
