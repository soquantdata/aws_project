from setuptools import setup

setup(
 name="database_module",
 version="0.1",
 packages=['database_module'],
 install_requires=['sqlalchemy==1.3.9','mysql-connector-python==8.0.21','pymssql==2.1.4','pandas==0.25.3','PyMySQL==0.9.3',
 'pymysql==0.9.3', 'psycopg2==2.8.6', 'sharepy==1.3.0', 'xlrd==1.2.0', 'pyarrow==0.14.1', 'pyodbc==4.0.30']
)

#python setup.py bdist_egg