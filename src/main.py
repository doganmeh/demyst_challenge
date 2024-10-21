import csv
import os
import shutil
import logging
import subprocess

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

faker_ = Faker()  # initialize faker

NUM_ROWS = 3_000_000
CHUNK_ROWS = 100_000
NUM_EXECUTORS = 1
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


def concat_csv_files(temp_dir: str, dest_file: str) -> None:
    first = True
    for source_file in os.listdir(temp_dir):
        if source_file.startswith("part-"):
            if first:
                command = f"cat {temp_dir}/{source_file} > {dest_file}"  # rewrite file including the header
                first = False
            else:
                command = f"tail -n +2 -q {temp_dir}/{source_file} >> {dest_file}"  # append skipping the header

            # to concatenate csv files
            try:
                subprocess.run(command, shell=True, check=True)
                logger.info(f"Executed the command successfully: {command}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error concatenating part files: {e}")
    shutil.rmtree(temp_dir)


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
    temp_dir = f'{PATH}/{output_file}'
    df_anonymized.write.option("header", "true").csv(temp_dir, mode='overwrite')

    # rename the part file to the desired output file name
    dest_file = f'{PATH}/{output_file}.csv'
    concat_csv_files(temp_dir, dest_file)
    logger.info(f"Finished anonymizing data to {output_file}.csv")


# generate rows in a csv file
# logger.info("Starting CSV generation")
# try:
#     generate_csv('people_data.csv')
# except Exception as e:
#     logger.error(f"Error generating CSV: {e}")
# logger.info("CSV generation completed")

# anonymize the generated csv file
logger.info("Starting CSV anonymization with PySpark")
try:
    anonymize_csv_pyspark('people_data.csv', 'anonymized_people_data')
except Exception as e:
    logger.error(f"Error anonymizing CSV: {e}")
logger.info("CSV anonymization completed")
