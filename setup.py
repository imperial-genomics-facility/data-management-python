import os
import sys
from setuptools import setup

tests_require = ["unittest2"]

base_dir = os.path.dirname(os.path.abspath(__file__))

version = "0.0.1"

setup(
    name = "data-management-python",
    version = version,
    description = "A python library for data analysis",
    long_description="\n\n".join([
        open(os.path.join(base_dir, "README.md"), "r").read(),
    ]),
    url = "https://github.com/imperial-genomics-facility/data-management-python",
    author = "Avik Datta",
    author_email = "reach4avik@yahoo.com",
    maintainer = "Avik Datta",
    maintainer_email = "reach4avik@yahoo.com",
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Testing',
    ],
    packages = ["data-management-python"],
    zip_safe = False,
    tests_require = tests_require,
    test_suite = "tests.get_tests",
)
