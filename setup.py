# setup.py

from setuptools import setup, find_packages

setup(
    name="spandas",
    version="0.1.0",
    description="Enhanced pandas-on-Spark DataFrame with pandas-like API and swifter/matplotlib support",
    author="Zeusu Sato",
    url="https://github.com/zeusu-sato/spandas",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.2.0",
        "swifter",
        "matplotlib"
    ],
    python_requires=">=3.7",
)
