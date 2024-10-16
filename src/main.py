import csv
import os
import shutil
import logging

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

faker_ = Faker()  # initialize faker

NUM_ROWS = 30_000
CHUNK_ROWS = 10_000
NUM_EXECUTORS = 2
PATH = "data"


# function to generate csv data
def generate_csv(file_name: str) -> None:
    """
    Generates a CSV file with fake personal data.

    Args:
        file_name (str): The name of the CSV file to be created.
    """
    logger.info(f"Generating {NUM_ROWS} rows of fake data into {file_name}")
    with open(f'{PATH}/{file_name}', mode='w', newline='') as file:
        writer = csv.writer(file)

        # write header
        writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])

        # write rows with generated data
        for _ in range(0, NUM_ROWS, CHUNK_ROWS):
            data = [
                [
                    faker_.first_name(),
                    faker_.last_name(),
                    faker_.address().replace('\n', ', '),
                    faker_.date_of_birth()
                ]
                for _ in range(min(CHUNK_ROWS, NUM_ROWS))
            ]
            writer.writerows(data)
    logger.info(f"Finished generating data into {file_name}")


# function to anonymize csv data using hashing with pyspark dataframe
def anonymize_csv_pyspark(input_file: str, output_file: str) -> None:
    """
    Anonymizes the personal data in a CSV file using hashing with PySpark DataFrame.

    Args:
        input_file (str): The name of the input CSV file to be anonymized.
        output_file (str): The name of the output CSV file with anonymized data.
    """
    logger.info(f"Anonymizing data from {input_file} to {output_file} using PySpark DataFrame approach")
    # initialize spark session
    spark = SparkSession.builder.appName("CSV Anonymization").master(f"local[{NUM_EXECUTORS}]").getOrCreate()
    # read the csv file into a dataframe
    df = spark.read.option("header", "true").csv(f'{PATH}/{input_file}')

    # anonymize the data using the udf
    df_anonymized = df \
        .withColumn("first_name", sha2(df["first_name"], 256)) \
        .withColumn("last_name", sha2(df["last_name"], 256)) \
        .withColumn("address", sha2(df["address"], 256))

    # write the anonymized dataframe to a new csv file
    df_anonymized.write.option("header", "true").csv(f'{PATH}/{output_file}', mode='overwrite')

    # rename the part file to the desired output file name
    temp_dir = f'{PATH}/{output_file}'
    for file_name in os.listdir(temp_dir):
        if file_name.startswith("part-"):
            shutil.move(os.path.join(temp_dir, file_name), f'{PATH}/{output_file}.csv')
    shutil.rmtree(temp_dir)
    logger.info(f"Finished anonymizing data to {output_file}.csv")


# generate rows in a csv file
logger.info("Starting CSV generation")
try:
    generate_csv('people_data.csv')
except Exception as e:
    logger.error(f"Error generating CSV: {e}")
logger.info("CSV generation completed")

# anonymize the generated csv file
logger.info("Starting CSV anonymization with PySpark")
try:
    anonymize_csv_pyspark('people_data.csv', 'anonymized_people_data')
except Exception as e:
    logger.error(f"Error anonymizing CSV: {e}")
logger.info("CSV anonymization completed")
