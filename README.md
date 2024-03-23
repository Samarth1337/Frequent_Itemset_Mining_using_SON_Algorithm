# Frequent_Itemset_Mining_using_SON_Algorithm_Recommendation_System

## Overview:
In this project, I implemented the SON (Savasere, Omiecinski, and Navathe) Algorithm using the Spark framework. The goal was to efficiently find frequent itemsets in two datasets: one simulated and one real-world dataset. The project focused on the Foundations and Applications of Data Mining.

## Problem Framing:
The task involved implementing the SON Algorithm to identify frequent itemsets efficiently in large datasets distributed across a Spark cluster. Two specific tasks were addressed:

### Task 1: Simulated Data Analysis

Implemented the SON Algorithm to find frequent businesses and users in a simulated CSV file.
Utilized Python and Spark RDD to process the data and identify frequent itemsets.
Leveraged market-basket modeling techniques to analyze user-business interactions.
Generated baskets for each user containing business IDs reviewed by them and vice versa.
Calculated combinations of frequent businesses and users based on predefined support thresholds.

### Task 2: Real-World Data Analysis (Ta Feng Dataset)

Preprocessed the Ta Feng dataset to aggregate purchases made by customers into daily baskets.
Applied the SON Algorithm to identify frequent itemsets in the preprocessed dataset.
Filtered out customers with a threshold number of purchases to focus on significant transactions.
Implemented a data pipeline to process the raw Ta Feng data efficiently using Spark RDD.
Generated a dataset with customer IDs and product IDs for each transaction.

## Technologies Used:
Programming Languages: Python (for implementation)

Framework: Apache Spark (using Spark RDD)

Libraries: PySpark, itertools, collections

Tools: SparkContext, Spark-submit

Data Handling: CSV parsing, data preprocessing

Data Structures: Sets, Tuples, Lists

## Dataset:
Simulated Dataset: Consisted of CSV files containing user-business interactions.

Ta Feng Dataset: Real-world transaction data retrieved from Kaggle, containing customer IDs and product IDs for each purchase.

## Solution Highlights:

### Efficient Algorithm Implementation: 
Implemented the SON Algorithm to handle large-scale data processing efficiently using Spark RDD.

### Market-Basket Modeling: 
Utilized market-basket modeling techniques to analyze user-item interactions and identify frequent itemsets.
### Data Preprocessing: 
Developed preprocessing steps to aggregate daily transactions from the raw Ta Feng dataset into baskets for analysis.
### Scalable Architecture: 
Leveraged the distributed computing capabilities of Apache Spark to process data in parallel across a cluster.

## Evaluation:
The project was evaluated based on the correctness of the implementation, adherence to programming requirements, and efficient handling of large datasets.
Execution time and scalability were important factors in evaluating the efficiency of the solution.

## Conclusion:
This project demonstrated the application of advanced data mining techniques, specifically the SON Algorithm, to identify frequent itemsets in large datasets. By leveraging the distributed computing capabilities of Apache Spark, the solution provided efficient processing of both simulated and real-world transaction data.
