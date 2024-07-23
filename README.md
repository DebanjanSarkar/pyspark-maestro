# PySpark Data Processing and Machine Learning Repository
------------------------------------------------------------------------------------------------------------------

Welcome to my PySpark repository! This repository is a comprehensive collection of PySpark code, Jupyter notebooks, and resources aimed at demonstrating various aspects of data processing, streaming, spark optimizations and machine learning using PySpark. It is designed for both beginners and experienced developers who want to learn and understand the capabilities of PySpark in real-world scenarios.

This repository contains code solutions designed by me, as well as certain material and resources from the internet that provides specific solutions for the specific scenarios.


## Table of Contents
------------------------------------------------------------------------------------------------------------------
1. [Introduction](#introduction)
2. [Features](#features)
3. [Setup Instructions](#setup-instructions)
4. [Batch Data Processing](#batch-data-processing)
5. [Kafka Integration](#kafka-integration)
6. [Random Data Generation and Kafka Publishing](#random-data-generation-and-kafka-publishing)
7. [Streaming Data Processing](#streaming-data-processing)
8. [Machine Learning Use Cases](#machine-learning-use-cases)
9. [Spark Optimization](#spark-optimization)
10. [Resources and References](#resources-and-references)
11. [Contributing](#contributing)
12. [License](#license)


## Introduction
------------------------------------------------------------------------------------------------------------------
This repository contains a series of Jupyter notebooks and Python scripts developed as part of my learning process in handling various data processing and machine learning tasks using PySpark. The notebooks are designed to be beginner-friendly with detailed explanations, step-by-step instructions, and accompanying code snippets. The repository includes everything needed to replicate the environment and follow along with the notebooks.


## Features
------------------------------------------------------------------------------------------------------------------
- **Batch Data Processing**: Learn how to process large datasets efficiently using PySpark.
- **Kafka Integration**: Set up and integrate Kafka with PySpark for real-time data processing.
- **Random Data Generation**: Automate random data generation and publish to Kafka to simulate real-world scenarios.
- **Streaming Data Processing**: Process streaming data from sources like Kafka and sockets.
- **Machine Learning**: Implement regression, classification, and other machine learning models using PySpark.
- **Spark Optimization**: Learn techniques to optimize Spark jobs for better performance.
- **Detailed Notes and Code Snippets**: Comprehensive explanations and code snippets for each notebook.
- **Setup Instructions**: Step-by-step setup instructions to replicate the environment.


## Setup Instructions
------------------------------------------------------------------------------------------------------------------
To get started, follow these steps to set up the environment and run the notebooks:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/DebanjanSarkar/pyspark-maestro.git
   cd pyspark-maestro
   ```

2. **Install Dependencies**:
   Ensure you have Python and Java installed. Then, create a virtual environment and install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```
   Certain more python packages are required to be installed for some specific notebooks. The installation and setup of those packages are described in the notebook itself, and can be done later during specific notebook code execution.

   For execution of these notebooks, `Spark` and `Hadoop` must be installed and configured in the local system. These notebooks are tested for Spark v3.3.2. Following environment variables must be set according to the installed path of spark, python, Hadoop and Java:
	```bash
	SPARK_HOME
	PYSPARK_HOME
	HADOOP_HOME
	JAVA_HOME
	```


3. **Set Up Kafka, Sockets and more**:
   For setting up Kafka, sockets, and more, the instructions are given in respective notebooks in details, and following that, environment setup could be done easily.

4. **Run Jupyter Notebooks**:
   Start Jupyter Notebook or Jupyter Lab and open the desired notebook:
   ```bash
   jupyter notebook
   ```
   OR
   ```bash
   jupyter lab
   ```


## Batch Data Processing
Explore batch data processing techniques using PySpark with detailed examples and code snippets. [View Notebooks](001-batch-data-processing/)

## Random Data Generation and Kafka Publishing
Automate the generation of random data and publish it to Kafka topics to simulate real-world data streams. [View Scripts](004-structured_streaming/python_util_scripts/generate_events_data_and_post_to_kafka)

## Streaming Data Processing
Process and analyze streaming data from various sources like Kafka and sockets using PySpark. [View Notebooks](004-structured_streaming/)

## Machine Learning Use Cases
Implement and evaluate machine learning models such as regression and classification using PySpark MLlib. [View Notebooks](003-spark_mlib/)

## Spark Optimization
Learn and apply various Spark optimization techniques to improve the performance of your Spark jobs. [View Notebooks](005-spark_optimizations/)


## Resources and References
------------------------------------------------------------------------------------------------------------------
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Jupyter Notebook Documentation](https://jupyter-notebook.readthedocs.io/)


## Contributing
------------------------------------------------------------------------------------------------------------------
Contributions are welcome! If you have any suggestions or improvements, please open an issue or submit a pull request.


## Author
------------------------------------------------------------------------------------------------------------------
[Debanjan Sarkar](https://github.com/DebanjanSarkar/)

For respective owners of certain code snippets, the respective authors and creators have been cited.
