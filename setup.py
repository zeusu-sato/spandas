from setuptools import setup, find_packages

setup(
    name='spandas',
    version='0.1.0',
    description='Enhanced pandas-like interface for PySpark',
    author='Zeusu Sato',
    author_email='zeusu.sato@dorodango.biz',
    url='https://github.com/zeusu-sato/spandas',
    packages=find_packages(include=["spandas", "spandas.*"]),
    install_requires=[
        'pyspark>=4.0.0',
        'pandas>=2.0',
        'numpy>=1.22',
        'swifter',
        'matplotlib',
        'pyarrow>=11.0.0',
    ],
    python_requires='>=3.7',
)
