"""
explore-covid: A package for exploring COVID-19 data using DuckDB
"""

from .database import create_covid_database

__version__ = "0.1.0"
__all__ = ["create_covid_database"]
