# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import requests


def is_package_published(package_name, version, test_pypi=False):
    """
    Check if a package with a specific version is published on PyPI or Test PyPI.

    :param package_name: The name of the package to check.
    :param version: The version of the package.
    :param test_pypi: Set to True to check on Test PyPI, False for regular PyPI.
    :return: True if the package is found with the specified version, False otherwise.
    """
    base_url = "https://test.pypi.org/pypi" if test_pypi else "https://pypi.org/pypi"
    url = f"{base_url}/{package_name}/{version}/json"

    response = requests.get(url)
    return response.status_code == 200