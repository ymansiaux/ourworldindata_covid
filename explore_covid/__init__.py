"""
explore-covid: A package for exploring COVID-19 data using DuckDB
"""

from .database import create_covid_database, database_exists, connect_to_db
from .get_data import get_countries_data
from .utils import get_package_root, get_data_dir, get_db_dir, get_resource_path

__version__ = "0.1.0"
# __all__ defines what gets imported when someone does "from explore_covid import *"
# It explicitly lists the public API functions that should be available to users
__all__ = [
    "create_covid_database",
    "database_exists",
    "connect_to_db",
    "get_package_root",
    "get_data_dir",
    "get_db_dir",
    "get_resource_path",
    "get_countries_data",
]
