import duckdb
import polars as pl
from pathlib import Path


def create_covid_database():
    """
    Create a DuckDB database with multiple tables from the OWID COVID data
    Tables: ref_table, df_cases, df_deaths, df_hospital, df_tests, df_vaccinations, df_social
    """

    # Connect to DuckDB (creates the database file)

    # Remove existing database if it exists
    db_path = Path("db/covid_data.duckdb")
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect("db/covid_data.duckdb")

    print("Loading COVID data...")
    # Load the main dataset
    df = pl.read_csv("data/owid-covid-data.csv").with_columns(
        pl.col("date").str.to_date("%Y-%m-%d")
    )

    print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    print("Creating tables...")

    # 1. REF_TABLE - Reference table with country/location information

    ref_table = (
        df.select(["iso_code", "continent", "location"])
        .unique(maintain_order=False)
        .sort("iso_code")
        .with_row_index("id", offset=1)
    )
    conn.execute("DROP TABLE IF EXISTS ref_table")
    conn.register("ref_table_temp", ref_table)
    conn.execute("CREATE TABLE ref_table AS SELECT * FROM ref_table_temp")

    df_with_id = df.join(
        ref_table, on=["iso_code", "continent", "location"], how="left"
    ).drop(["iso_code", "continent", "location"])

    # 2. DF_CASES - Cases data
    cases_columns = [
        "id",
        "date",
        "total_cases",
        "new_cases",
        "new_cases_smoothed",
        "total_cases_per_million",
        "new_cases_per_million",
        "new_cases_smoothed_per_million",
        "reproduction_rate",
    ]

    df_cases = df_with_id.select(cases_columns).drop_nulls(
        subset=["total_cases", "new_cases"]
    )
    conn.execute("DROP TABLE IF EXISTS df_cases")
    conn.register("df_cases_temp", df_cases)
    conn.execute("CREATE TABLE df_cases AS SELECT * FROM df_cases_temp")

    # 3. DF_DEATHS - Deaths data
    deaths_columns = [
        "id",
        "date",
        "total_deaths",
        "new_deaths",
        "new_deaths_smoothed",
        "total_deaths_per_million",
        "new_deaths_per_million",
        "new_deaths_smoothed_per_million",
        "excess_mortality_cumulative_absolute",
        "excess_mortality_cumulative",
        "excess_mortality",
        "excess_mortality_cumulative_per_million",
    ]

    df_deaths = df_with_id.select(deaths_columns).drop_nulls(
        subset=["total_deaths", "new_deaths"]
    )
    conn.execute("DROP TABLE IF EXISTS df_deaths")
    conn.register("df_deaths_temp", df_deaths)
    conn.execute("CREATE TABLE df_deaths AS SELECT * FROM df_deaths_temp")

    # 4. DF_HOSPITAL - Hospital data
    hospital_columns = [
        "id",
        "date",
        "icu_patients",
        "icu_patients_per_million",
        "hosp_patients",
        "hosp_patients_per_million",
        "weekly_icu_admissions",
        "weekly_icu_admissions_per_million",
        "weekly_hosp_admissions",
        "weekly_hosp_admissions_per_million",
    ]

    df_hospital = df_with_id.select(hospital_columns).drop_nulls(
        subset=["icu_patients", "hosp_patients"]
    )
    conn.execute("DROP TABLE IF EXISTS df_hospital")
    conn.register("df_hospital_temp", df_hospital)
    conn.execute("CREATE TABLE df_hospital AS SELECT * FROM df_hospital_temp")

    # 5. DF_TESTS - Testing data
    tests_columns = [
        "id",
        "date",
        "total_tests",
        "new_tests",
        "total_tests_per_thousand",
        "new_tests_per_thousand",
        "new_tests_smoothed",
        "new_tests_smoothed_per_thousand",
        "positive_rate",
        "tests_per_case",
        "tests_units",
    ]

    df_tests = df_with_id.select(tests_columns).drop_nulls(
        subset=["total_tests", "new_tests"]
    )
    conn.execute("DROP TABLE IF EXISTS df_tests")
    conn.register("df_tests_temp", df_tests)
    conn.execute("CREATE TABLE df_tests AS SELECT * FROM df_tests_temp")

    # 6. DF_VACCINATIONS - Vaccination data
    vaccinations_columns = [
        "id",
        "date",
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "total_boosters",
        "new_vaccinations",
        "new_vaccinations_smoothed",
        "total_vaccinations_per_hundred",
        "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred",
        "total_boosters_per_hundred",
        "new_vaccinations_smoothed_per_million",
        "new_people_vaccinated_smoothed",
        "new_people_vaccinated_smoothed_per_hundred",
    ]

    df_vaccinations = df_with_id.select(vaccinations_columns).drop_nulls(
        subset=["total_vaccinations", "people_vaccinated"]
    )
    conn.execute("DROP TABLE IF EXISTS df_vaccinations")
    conn.register("df_vaccinations_temp", df_vaccinations)
    conn.execute("CREATE TABLE df_vaccinations AS SELECT * FROM df_vaccinations_temp")

    # 7. DF_SOCIAL - Social/policy measures
    social_columns = ["id", "date", "stringency_index"]

    df_social = df_with_id.select(social_columns).drop_nulls(
        subset=["stringency_index"]
    )
    conn.execute("DROP TABLE IF EXISTS df_social")
    conn.register("df_social_temp", df_social)
    conn.execute("CREATE TABLE df_social AS SELECT * FROM df_social_temp")

    # Print summary information
    print("\n=== DATABASE CREATED SUCCESSFULLY ===")
    print(f"Database file: {db_path}")

    tables_info = [
        ("ref_table", ref_table, "Country reference data"),
        ("df_cases", df_cases, "COVID-19 cases data"),
        ("df_deaths", df_deaths, "COVID-19 deaths data"),
        ("df_hospital", df_hospital, "Hospital and ICU data"),
        ("df_tests", df_tests, "Testing data"),
        ("df_vaccinations", df_vaccinations, "Vaccination data"),
        ("df_social", df_social, "Social distancing measures"),
    ]

    print("\nTables created:")
    for table_name, table_df, description in tables_info:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result else 0
        col_count = len(table_df.columns)
        print(
            f"  {table_name:15} - {row_count:8,} rows, {col_count:2} columns - {description}"
        )

    # Close the connection
    conn.close()

    return db_path


if __name__ == "__main__":
    create_covid_database()
