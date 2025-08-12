from setuptools import setup, find_packages

setup(
    name='spandas',
    version='0.2.0',
    description='Spark + pandas hybrid utilities',
    author='Zeusu Sato',
    author_email='zeusu.sato@dorodango.biz',
    url='https://github.com/zeusu-sato/spandas',
    packages=find_packages(include=["spandas", "spandas.*"]),
    install_requires=[
        'pandas>=1.5,<2.0',
        'numpy>=1.22,<2.0',
        'pyarrow>=8,<13',
        'matplotlib>=3.7,<3.8',
    ],
    extras_require={
        'spark': ['pyspark>=3.5,<3.6'],
        'dask_legacy': ['dask[dataframe]==2023.9.3'],
    },
    python_requires='>=3.10,<3.12',
)
