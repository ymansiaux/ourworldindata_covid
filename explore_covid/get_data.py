from explore_covid import connect_to_db
import polars as pl
import duckdb


def get_countries_data(
    db_content: duckdb.DuckDBPyConnection, countries_to_find: list[str]
) -> pl.DataFrame:
    """
    Get the data for the countries
    Args:
        countries_to_find: list of countries to find
    Returns:
        pl.DataFrame: dataframe with the data for the countries
    """
    countries_tuple = tuple(countries_to_find)
    countries_data = (
        db_content.execute(
            f"SELECT * FROM ref_table WHERE location IN {countries_tuple}"
        )
    ).pl()
    return countries_data
