from explore_covid import create_covid_database


def main():
    print("Hello from explore-covid!")
    print("Creating COVID-19 database...")
    db_file = create_covid_database()
    print(f"Database created: {db_file}")


if __name__ == "__main__":
    main()
