import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_unixtime, when, to_timestamp, lit

# Load environment variables from .env file
load_dotenv()

# Database configuration from environment variables
DB_CONFIG = {
    "url": os.getenv("DB_URL"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER")
}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MongoDB to PostgreSQL Migration") \
    .config("spark.jars.packages", os.getenv("SPARK_PACKAGES")) \
    .getOrCreate()


def read_mongo_data():
    try:
        mongo_df = spark.read.option("multiline", "true").json(os.getenv("MONGO_INPUT_FILE"))
        print("Successfully read data from MongoDB")
        print("MongoDB DataFrame Schema:")
        mongo_df.printSchema()
        print("Sample data from MongoDB:")
        mongo_df.show(5, truncate=False)
        return mongo_df
    except Exception as e:
        print(f"Error reading from MongoDB: {str(e)}")
        sys.exit(1)


def read_postgres_table(table_name):
    return spark.read \
        .format("jdbc") \
        .options(**DB_CONFIG) \
        .option("dbtable", f"kuma.{table_name}") \
        .load()


def convert_created_at(df):
    if "createdAt" in df.columns:
        if "$date" in df.select("createdAt.*").columns:
            return to_timestamp(from_unixtime(col("createdAt.$date") / 1000))
        else:
            return to_timestamp(col("createdAt"))
    else:
        return lit(None)


def main():
    # Read data from MongoDB
    mongo_df = read_mongo_data()

    # Read data from PostgreSQL
    postgres_term_df = read_postgres_table("term")
    category_df = read_postgres_table("grammatical_category")
    people_df = read_postgres_table("people")
    language_df = read_postgres_table("language")

    if None in [mongo_df, category_df, people_df, language_df]:
        print("One or more required DataFrames could not be read. Exiting.")
        spark.stop()
        sys.exit(1)

    # Join with category, people, and language tables
    joined_df = mongo_df.alias("m") \
        .join(category_df.alias("c"), mongo_df["category"] == category_df.name, "left") \
        .join(people_df.alias("p"), mongo_df["username"] == people_df.name, "left") \
        .join(language_df.alias("l"), mongo_df["name"] == language_df.name, "left") \
        .select(
        category_df.id.alias("grammatical_category_id"),
        people_df.id.alias("author_id"),
        language_df.id.alias("language_id"),
        col("m.name"),
        col("m.definition"),
        col("m.translation"),
        convert_created_at(mongo_df).alias("created_at")
    )

    # Write the transformed data to PostgreSQL
    joined_df.write \
        .format("jdbc") \
        .options(**DB_CONFIG) \
        .option("dbtable", "kuma.term") \
        .option("url", f"{DB_CONFIG['url']}?prepareThreshold=0") \
        .mode("append") \
        .save()

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
