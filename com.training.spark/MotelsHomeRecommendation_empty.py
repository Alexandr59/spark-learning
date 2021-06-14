import findspark
import sys

findspark.init()

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

ERRONEOUS_DIR = "erroneous"
AGGREGATED_DIR = "aggregated"


def get_raw_bids(session: SparkSession, bids_path: str) -> DataFrame:
    # TODO
    pass


def get_erroneous_records(bids: DataFrame) -> DataFrame:
    # TODO
    pass


def get_exchange_rates(session: SparkSession, path: str) -> DataFrame:
    # TODO
    pass


def get_bids(bids: DataFrame, rates: DataFrame) -> DataFrame:
    # TODO
    pass


def get_motels(session: SparkSession, path: str) -> DataFrame:
    # TODO
    pass


def get_enriched(bibs: DataFrame, motel: DataFrame) -> DataFrame:
    # TODO
    pass


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
