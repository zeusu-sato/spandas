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
    ],
    extras_require={
        'perf': ['tqdm>=4.33,<5'],
    },
    python_requires='>=3.10,<3.12',
)
