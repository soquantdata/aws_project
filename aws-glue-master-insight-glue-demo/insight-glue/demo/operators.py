import os
import sys
import boto3
from botocore.exceptions import NoCredentialsError
from boto3.dynamodb.conditions import Key
import gzip
from io import StringIO
from io import BytesIO
import sqlalchemy as sl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
import psycopg2
import pymssql
import pandas as pd
import pymysql
import sharepy
import pyarrow as pa
import pyarrow.parquet as pq
import io

#### UPLOAD LOCAL FILE TO S3
def upload_to_aws(client, local_file, bucket, s3_file):
    s3_resource = client
    try:
        s3_resource.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


##### WRITE DATAFRAME TO S3 LOCATION
def write_dataframe_to_csv_on_s3(client, dataframe, bucket, filepath):
    s3_resource = client
    """ Write a dataframe to a CSV on S3 """
    print("Writing {} records to S3 File: {}".format(len(dataframe), filepath))
     # Create buffer
    csv_buffer = StringIO()
    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep="|",  index=False)
    # Write buffer to S3 object
    s3_resource.Object(bucket, filepath).put(Body=csv_buffer.getvalue())


##### MOVE AND DELETE PROCESSED S3 FILE
def move_and_delete_s3_to_s3(client, target_bucket, target_filepath, source_bucket, source_filepath):
    my_s3_path = source_bucket + '/' + source_filepath
    s3_resource = client ##Copy object A as object B
    s3_resource.Object(target_bucket, target_filepath).copy_from(CopySource=my_s3_path)# Delete the former object A
    s3_resource.Object(source_bucket, source_filepath).delete()


### MYSQL GET DATA
def run_mysql_query(sqlhost, sqldatabase, sqluser, sqlpassword, sql):
    conn = pymysql.connect(host=sqlhost, user=sqluser, password=sqlpassword, database=sqldatabase)
    db_cursor = conn.cursor()
    db_cursor.execute(sql)
    table_rows = db_cursor.fetchall()
    table_df = pd.DataFrame(table_rows)
    conn.close()
    return table_df


#### GET DATA FROM REDSHIFT
def get_redshhift_connection(host):
    rs_conn_string = "host=%s port=%s dbname=%s user=%s password=%s" % (
        host, port, db_name, user, password_for_user)

    rs_conn = pg.connect(dbname=rs_conn_string)
    rs_conn.query("set statement_timeout = 1200000")
    return rs_conn


def redshift_query(con, sql):
    statement = sql
    res = con.query(statement)
    return res


def redshift_to_pandas(host, port, user, password, database, sql):
    # pass a sql query and return a pandas dataframe
    con = psycopg2.connect(dbname = database, host = host, port = port, user = user, password = password)
    cursor = con.cursor()
    cursor.execute(sql)
    columns_list = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=columns_list)
    # data = data.fillna(0)
    return data
    
def execute_Redshift_SQL(host, port, user, password, database, sql):
    con = psycopg2.connect(dbname= database, host= host, port= port, user= user, password= password)
    cursor = con.cursor()
    cursor.execute(sql)
    con.commit()


### GET DATA FROM SQL SERVER
def connect_mssql_db(user, password, host, port, database, sql, connection_type = None):
    engine = sl.create_engine(
    'mssql+pymssql://'+str(user)+':'
    +str(password)+'@'
    +str(host)
    +':'+str(port)+'/'
    +str(database),
    encoding="utf8",
    convert_unicode=True
    )
    connection = engine.raw_connection() if connection_type == "raw" else engine.connect()
    tbl_rows = connection.execute(sql)  
    tbl_df = pd.DataFrame(tbl_rows)
    return tbl_df

### GET DATA FROM SQL SERVER
def execute_mssql_command(user, password, host, port, database, sql, connection_type = None):
    try:
        engine = sl.create_engine(
        'mssql+pymssql://'+str(user)+':'
        +str(password)+'@'
        +str(host)
        +':'+str(port)+'/'
        +str(database),
        encoding="utf8",
        convert_unicode=True
        )
        connection = engine.connect()
        connection = connection.execution_options(autocommit=True)
        connection.execute(sql)
        connection.close()
        print("Finished Successfully")
    except:
        print("Error - Exited with Error")
    

### GET DATA FROM DYNAMODB
def dynamodb_to_pandas(client, table_name, Lastupdate):
    dynamo_client = client
    table = dynamo_client.Table(table_name)
    records = table.scan(FilterExpression= Key('LastUpdate').gte(Lastupdate))
    df = pd.DataFrame.from_dict(records["Items"])
    return df


### COPY EXCEL FROM SHAREPOINT TO AWS S3
def read_from_sharepoint(client, spuser, sppassword, spdomain, spfileurl, awsbucket, awspath):
    s3_resource = client
    # (1) Authenticate
    s = sharepy.connect(spdomain, username=spuser, password=sppassword)
    # (2) fetch file content
    r = s.get(spfileurl);
    if(r.status_code !=200):
        print(r.text)
    # (2) upload file content to s3
    if(r.status_code ==200):
        s3_response = s3_resource.Object(awsbucket, awspath).put(Body=r.content)
        if s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("File uploaded to s3");
        else:
            print(s3_response["ResponseMetadata"]);

### READ EXCEL FROM AWS S3 USING PANDAS - SPLIT BY SHEETNAME
def read_excel_s3(s3client, s3resource, bucket, filepath, destpath):
    s3_client = s3client
    s3_resource = s3resource
    obj = s3_client.get_object(Bucket=bucket, Key=filepath)
    data = obj['Body'].read()
    xls = pd.ExcelFile(io.BytesIO(data))
    sheets = xls.sheet_names
    for sheet in sheets:
        result = pd.read_excel(io.BytesIO(data), sheet_name=sheet, encoding='utf-8')
        target_path = destpath.format(sheet, sheet)
        s3_write = write_dataframe_to_csv_on_s3(s3_resource, result, bucket, target_path)
        print("Sheet '{}' is uploaded to {}".format(sheet, target_path))

### Create Parquet or CSV file and load into AWS S3 using Pandas

def write_dataframe_to_s3(client, dataframe, bucket, filepath, outputfile_format):
    s3_resource = client
    if outputfile_format == 'parquet':
        print("Writing {} records to S3 File: {}".format(len(dataframe), filepath))
        out_buffer = BytesIO()
        # table = pa.Table.from_pandas(dataframe)
        # pq.write_table(table, fileName)
        dataframe.to_parquet(out_buffer, index=False)
        print('came inside of  parquet format')

    elif outputfile_format == 'csv':
        out_buffer = StringIO()
        dataframe.to_csv(out_buffer, sep="|",  index=False)
        print('came inside of  csv format')
    s3_resource.Object(bucket, filepath).put(Body=out_buffer.getvalue())


## Get data from the Azure Sql Database 
def run_azure_sql_query(driver,host,database,username,password,sql):
    print(driver,host,database,username,password)
 
    database_uri = "mssql+pyodbc:///?odbc_connect={}";
    connStr = 'DRIVER='+driver+';PORT=1433;SERVER='+host+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password;
    connEncodedStr = urllib.parse.quote(connStr);
    engine = sl.create_engine(database_uri.format(connEncodedStr),module=pypyodbc);
    cur = engine.connect();
    table_df=pd.read_sql(sql,cur)
    table_df[['success','completion']]=table_df[['success','completion']].astype(int)
    print(table_df)
    print("successful")
    cur.close()
    if table_df is 'empty':
        return False
    else:
        return table_df



# def write_partitioned_dataset_to_s3():
#     pq.write_to_dataset(table, root_path=SOURCE_S3_PATH1, partition_cols=['Year','Month','Day'], flavor='spark',filesystem=s3)




    #client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())


# def write_pandas_parquet_to_s3(dataframe, bucketName, keyName, fileName):
#     # dummy dataframe
#     table = pa.Table.from_pandas(dataframe)
#     pq.write_table(table, fileName)

#     # upload to s3
#     s3 = boto3.client("s3")
#     BucketName = bucketName
#     with open(fileName) as f:
#        object_data = f.read()
#        s3.put_object(Body=object_data, Bucket=BucketName, Key=keyName)

