import findspark
import sys
from constants import *

findspark.init()

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

ERRONEOUS_DIR = "erroneous"
AGGREGATED_DIR = "aggregated"


def get_raw_bids(session: SparkSession, bids_path: str) -> DataFrame:
    schema = StructType([StructField(col_name, StringType(), True) for col_name in BIDS_HEADER[:3]])
    for i in BIDS_HEADER[3:]:
        schema.fields.append(StructField(i, DecimalType(5, 3), True))
    return session.read.schema(schema).csv(bids_path)


def get_erroneous_records(bids: DataFrame) -> DataFrame:
    bids = bids.filter(bids.HU.rlike("ERROR_(.*)")) \
        .select(BIDS_HEADER[1], BIDS_HEADER[2]) \
        .withColumnRenamed(BIDS_HEADER[2], 'errorMessage') \
        .withColumnRenamed(BIDS_HEADER[1], 'hour')
    bids = bids.withColumn('hour', to_timestamp(col('hour'), 'HH-dd-MM-yyyy'))
    bids = bids.withColumn('hour', hour(col('hour')))
    return bids.groupby('hour', 'errorMessage').count()


def get_exchange_rates(session: SparkSession, path: str) -> DataFrame:
    schema = StructType([StructField(col_name, StringType(), True) for col_name in EXCHANGE_RATES_HEADER[:-1]])
    schema.fields.append(StructField(EXCHANGE_RATES_HEADER[3], DecimalType(5, 3), True))
    return session.read.schema(schema).csv(path).select(EXCHANGE_RATES_HEADER[0], EXCHANGE_RATES_HEADER[3])


def get_bids(bids: DataFrame, rates: DataFrame) -> DataFrame:
    bids = bids.select(*BIDS_HEADER[:2], *TARGET_LOSAS)
    bids = bids.dropna()
    bids = bids.join(rates, bids[BIDS_HEADER[1]] == rates[EXCHANGE_RATES_HEADER[0]])
    for country in TARGET_LOSAS:
        bids = bids.withColumn(country, (col(country) * col(EXCHANGE_RATES_HEADER[3])).cast(DecimalType(5, 3)))
    bids = bids.withColumn(BIDS_HEADER[1], to_timestamp(col(BIDS_HEADER[1]), 'HH-dd-MM-yyyy'))
    return bids.select(*BIDS_HEADER[:2], expr("stack(3, 'US', US, 'CA', CA, 'MX', MX) as (Losa,Price)"))


def get_motels(session: SparkSession, path: str) -> DataFrame:
    schema = StructType([StructField(col_name, StringType(), True) for col_name in MOTELS_HEADER])
    return session.read.schema(schema).csv(path).select(MOTELS_HEADER[:2])


def get_enriched(bibs: DataFrame, motel: DataFrame) -> DataFrame:
    bibs = bibs.join(motel, BIDS_HEADER[0])
    return bibs.groupby(BIDS_HEADER[0], BIDS_HEADER[1]).agg(max('Price').alias('Price')) \
        .join(bibs, [*BIDS_HEADER[0:2], 'Price'])


def process_data(session: SparkSession, bids_path: str, motels_path: str, exchange_rates_path: str,
                 output_base_path: str):
    """
    Task 1:
        * Read the bid data from the provided file
      """
    raw_bids = get_raw_bids(session, bids_path)
    raw_bids.show(10)

    """
    * Task 2:
      * Collect the errors and save the result.
    """
    erroneous_records = get_erroneous_records(raw_bids)
    erroneous_records.show(5)

    erroneous_records \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .csv(output_base_path + "/" + ERRONEOUS_DIR)

    """
    * Task 3:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
    """
    exchange_rates = get_exchange_rates(session, exchange_rates_path)

    """
    * Task 4:
      * Transform the rawBids.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
    """

    bids = get_bids(raw_bids, exchange_rates)

    """
    * Task 5:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
    """
    motels = get_motels(spark_session, motels_path)

    """
    * Task6:
      * Join the bids with motel names.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price
    """

    enriched = get_enriched(bids, motels)

    enriched.coalesce(1) \
        .write.csv(output_base_path + "/" + AGGREGATED_DIR)


if __name__ == '__main__':
    spark_session = SparkSession \
        .builder \
        .appName("motels-home-recommendation") \
        .master("local[*]") \
        .getOrCreate()

    if len(sys.argv) == 5:
        bidsPath = sys.argv[1]
        motelsPath = sys.argv[2]
        exchangeRatesPath = sys.argv[3]
        outputBasePath = sys.argv[4]
    else:
        raise ValueError("Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    process_data(spark_session, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    spark_session.stop()
