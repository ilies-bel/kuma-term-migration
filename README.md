# MongoDB to PostgreSQL Migration Spark Job

## Overview

This project contains a PySpark job that migrates data from MongoDB to PostgreSQL. It reads data from a MongoDB export file, transforms it, and writes it to specified tables in a PostgreSQL database. The job also performs joins with existing PostgreSQL tables to enrich the data during the migration process.

## Features

- Reads data from a MongoDB JSON export file
- Connects to PostgreSQL database to read and write data
- Performs joins with existing PostgreSQL tables (grammatical_category, people, language)
- Transforms data, including date conversions
- Configurable through environment variables for easy deployment across different environments

## Prerequisites

- Python 3.7+
- Apache Spark 3.x
- PostgreSQL database
- MongoDB JSON export file

## Getting Started

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/mongodb-to-postgresql-migration.git
   cd mongodb-to-postgresql-migration
   ```

2. Install the required Python packages:
   ```
   pip install pyspark python-dotenv
   ```

3. Create a `.env` file in the project root directory with the following content:
   ```
   # Database Configuration
   DB_URL=jdbc:postgresql://your_host:your_port/your_database
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_DRIVER=org.postgresql.Driver

   # MongoDB Configuration
   MONGO_INPUT_FILE=path/to/your/export_mongo1.json

   # Spark Configuration
   SPARK_PACKAGES=org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.18
   ```
   Replace the placeholder values with your actual database credentials and file paths.

4. Ensure your MongoDB JSON export file is in the specified location.

5. Run the Spark job:
   ```
   spark-submit --packages ${SPARK_PACKAGES} main.py
   ```

## Project Structure

- `main.py`: The main PySpark script that performs the migration
- `.env`: Configuration file for database credentials and other settings
- `README.md`: This file, containing project documentation

## How It Works

1. The script starts by loading configuration from the `.env` file.
2. It initializes a Spark session with the necessary packages for MongoDB and PostgreSQL connectivity.
3. Data is read from the MongoDB JSON export file.
4. The script then reads data from several PostgreSQL tables (term, grammatical_category, people, language).
5. It performs joins between the MongoDB data and PostgreSQL tables to enrich the data.
6. The enriched data is then written back to the PostgreSQL `kuma.term` table.

## Customization

You can customize the behavior of the script by modifying the `.env` file. This allows you to change database connections, input file locations, and Spark configurations without altering the code.

## Troubleshooting

- If you encounter connection issues, double-check the database credentials in your `.env` file.
- Ensure that the MongoDB JSON export file is in the correct format and location.
- Verify that all required PostgreSQL tables exist in your database.

## Contributing

Contributions to improve the script or extend its functionality are welcome. Please feel free to submit pull requests or open issues for any bugs or feature requests.
