"""
Utility functions for package path management
"""

import importlib.resources
from pathlib import Path
import explore_covid


def get_package_root():
    """
    Get the root directory of the explore_covid package
    Similar to system.file() in R
    """
    try:
        # Modern approach (Python 3.9+)
        package_path = importlib.resources.files(explore_covid)
        return Path(package_path)
    except AttributeError:
        # Fallback for older Python versions
        import pkg_resources

        return Path(pkg_resources.resource_filename("explore_covid", ""))


def get_data_dir():
    """
    Get the data directory relative to package root
    """
    package_root = get_package_root()
    # Go up one level from package to project root, then to data
    return package_root.parent / "data"


def get_db_dir():
    """
    Get the database directory relative to package root
    """
    package_root = get_package_root()
    # Go up one level from package to project root, then to db
    return package_root.parent / "db"


def get_resource_path(resource_name):
    """
    Get path to a specific resource file

    Args:
        resource_name (str): Name of the resource file

    Returns:
        Path: Full path to the resource
    """
    package_root = get_package_root()
    return package_root / resource_name
