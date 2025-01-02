# Airflow-Hourly Wikipedia Pageviews Pipeline

This project demonstrates the use of **Apache Airflow** to orchestrate the extraction, processing, and storage of hourly pageview data from Wikipedia. The pipeline fetches hourly pageview data, processes it to extract views for specific pages, and stores the results in a **PostgreSQL** database. All components are orchestrated and executed seamlessly using **Docker**.

---
## DAG

![DAG](Dag.png)


## Project Overview

### Key Features
- **Apache Airflow DAG**: Manages the sequence of tasks to ensure data processing flows efficiently.
- **Python and Bash Operators**: Utilizes Python for data processing and Bash for command execution.
- **PostgreSQL Database**: Stores extracted data for further analysis or visualization.
- **Dockerized Environment**: Ensures consistent execution across different systems with containerized services.

---

## Workflow Steps

1. **Download Wikipedia Pageview Data**
   - A **PythonOperator** downloads compressed pageview data for a specific hour using a dynamically constructed URL.
   - The URL is formatted based on the **execution_date** provided by Airflow.

2. **Unzip the Data**
   - A **BashOperator** unzips the downloaded `.gz` file to make the raw data accessible for processing.

3. **Extract Relevant Pageview Data**
   - A **PythonOperator** parses the unzipped file, extracting hourly pageview counts for a predefined set of popular pages (`Google`, `Amazon`, `Apple`, `Microsoft`, `Facebook`).
   - Constructs and saves a series of SQL `INSERT` statements to a file for database insertion.

4. **Write Data to PostgreSQL**
   - A **PostgresOperator** executes the SQL file to store the extracted data in a **PostgreSQL** table.

---

## Technologies Used

### **Apache Airflow**
- Orchestrates the pipeline using a Directed Acyclic Graph (DAG).
- Ensures tasks execute in the correct sequence with proper dependency handling.
- Key Parameters:
  - `dag_id`: Unique identifier for the DAG.
  - `start_date`: Determines when the pipeline begins execution.
  - `template_searchpath`: Allows dynamic templating of paths or queries.

### **Docker**
- Provides a containerized setup for Airflow, PostgreSQL, and other services.
- Ensures the pipeline runs seamlessly in any environment without dependency issues.

### **PostgreSQL**
- Stores processed pageview counts for further analysis.
- The data structure is dynamically generated via SQL scripts created during the data extraction step.

---
