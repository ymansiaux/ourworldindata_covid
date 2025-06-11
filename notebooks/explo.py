from explore_covid import create_covid_database, connect_to_db
import duckdb
import polars as pl

db_file = create_covid_database()

db_content = connect_to_db()
countries_to_find = ["France", "Germany", "Italy", "Spain", "United Kingdom"]

# Get the data for the countries
countries_tuple = tuple(countries_to_find)
countries_data = (
    db_content.execute(f"SELECT * FROM ref_table WHERE location IN {countries_tuple}")
).pl()

countries_data.head()

countries_data.filter(pl.col("location").is_in(countries_to_find))


# Get the data for the countries
