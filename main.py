from explore_covid import create_covid_database, connect_to_db, get_countries_data


def main():
    print("Hello from explore-covid!")
    print("Creating COVID-19 database...")
    db_file = create_covid_database(force_recreate=False)
    print(f"Database created: {db_file}")

    db_content = connect_to_db()
    countries_data = get_countries_data(
        db_content, ["France", "Germany", "Italy", "Spain", "United Kingdom"]
    )
    print(countries_data)


if __name__ == "__main__":
    main()
