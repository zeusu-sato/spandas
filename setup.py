from setuptools import setup, find_packages

setup(
    name='spandas',
    version='0.1.1',
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
        'swifter>=1.4,<1.5',
        'dask[dataframe]>=2024.2,<2024.7',
        'dask-expr>=1.0,<1.1',
    ],
    extras_require={
        'local': ['pyspark>=3.5,<3.6'],
        'dev': ['pytest>=7.4'],
        'dask_legacy': [],
    },
    python_requires='>=3.10,<3.12',
)
