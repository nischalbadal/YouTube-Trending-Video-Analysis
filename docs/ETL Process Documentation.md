# Youtube Trending Video Data Analysis
#### Final Assignment - Leapfrog Data Engineering Internship
 
 In this final assignment, we carry out all the process from designing the database, ETL of the raw database into the transformed and loaded final tables, designing the Data Warehouse, Validating using SQL test cases and finally visualizing the data using Microsoft PowerBI.
 
 The data for the project was obtained from [here](https://www.kaggle.com/datasnaek/youtube-new?select=USvideos.csv).

 
 In the ETL process, the following steps were done:
 - Initially, we extract the given raw data into their respective raw tables. 
    They are: _raw_category_ and _raw_video_.
- The raw data are in two file formats: JSON and CSV. We extract them into their respective raw tables using python and psycopg2 driver.
- The  raw tables do not have any specific data types as all of them are stored as string to avoid mismatching of date formats from the incoming raw data. It is afterwards casted into their suitable data types using cast() method of postgresql.
- We also create archive tables for each raw table to maintain a historic datawarehouse that contails all the records before transformation and loading processes.
- After successful extraction, we now transform the data to load into our fact and dimension tables. 
- After transformation, we now load the transformed data into respective dimension and fact tables. The order of loading is first to the dimension tables and then they are referenced to the fact tables as mentioned in the steps below.
- ###### Note: Each table except archive table is truncated initially to clear any previous values (if exists) and the ETL process could be completed without any referential constraint errors.

All of the above mentioned steps are furthur explained briefly below with their code and query snippets and screenshot of tables.

 # E - Extraction
We create two raw tables as per the data given named: _raw_category_ and _raw_video_. The DDL queries of individual tables are given below in the respective manner.

```sql
create table raw_category(
    id VARCHAR(500) UNIQUE,
    title VARCHAR(500),
    kind VARCHAR(500),
    etag VARCHAR(500),
    channel_id VARCHAR(500),
    assignable BOOLEAN,
    country VARCHAR(500)
);
```
```sql
create table raw_video(
    video_id VARCHAR(500),
    trending_date VARCHAR(500),
    title VARCHAR(5000),
    channel_title VARCHAR(500),
    category_id VARCHAR(500),
    publish_time VARCHAR(500),
    tags VARCHAR(5000),
    views VARCHAR(500),
    likes VARCHAR(500),
    dislikes VARCHAR(500),
    comment_count VARCHAR(500),
    thumbnail_link VARCHAR(500),
    comments_disabled VARCHAR(500),
    ratings_disabled VARCHAR(500),
    video_error_or_removed VARCHAR(500),
    description VARCHAR(15000),
    country VARCHAR(500)
);

```
Similarly, the archive tables are named as _archive_raw_category_ and _archive_raw_video_. 
Below is the DDL queries of archive tables.
```sql
create table archive_raw_category(
    id VARCHAR(500) UNIQUE,
    title VARCHAR(500),
    kind VARCHAR(500),
    etag VARCHAR(500),
    channel_id VARCHAR(500),
    assignable BOOLEAN,
    country VARCHAR(500)
);
```
```sql
create table archive_raw_video(
    video_id VARCHAR(500),
    trending_date VARCHAR(500),
    title VARCHAR(5000),
    channel_title VARCHAR(500),
    category_id VARCHAR(500),
    publish_time VARCHAR(500),
    tags VARCHAR(5000),
    views VARCHAR(500),
    likes VARCHAR(500),
    dislikes VARCHAR(500),
    comment_count VARCHAR(500),
    thumbnail_link VARCHAR(500),
    comments_disabled VARCHAR(500),
    ratings_disabled VARCHAR(500),
    video_error_or_removed VARCHAR(500),
    description VARCHAR(15000),
    country VARCHAR(500)
);
```
After creating the raw tables and their archive tables, we now perform insertion into the tables using python script. The database is connected using ```psycopg2``` driver for python3. The database connection script is:

```python
import psycopg2
def connect():
        return psycopg2.connect(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("database")
        )
```

The Extraction process of the _raw_category_ table contains:

```python
 def extract_category_data(fileName, con, cur):
        with open(fileName, 'r') as f:
                country_list = fileName.split('/')[3]
                country = country_list[0:-17]

                record_list = json.load(f)
                formatted_list=[]
                for i in range(len(record_list['items'])):
                    formatted_list.append([
                            record_list['items'][i]['id'],
                            record_list['items'][i]['snippet']['title'],
                            record_list['items'][i]['kind'],
                            record_list['items'][i]['etag'],
                            record_list['items'][i]['snippet']['channelId'],
                            record_list['items'][i]['snippet']['assignable'],
                            country
                          ])
                for row in formatted_list:
                    with open("../sql/queries/extract_raw_category_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query, row)       
                        con.commit()
        print("Extraction of "+country+" data successful to raw_category table.") 
```
Here, as we can see the raw data files are country specific. i.e. There is a country code in the filename. The country code is extracted and inserted as a new column called country. The JSON file is loaded using ```json``` library of python and ```json.load()``` method.

The loop iterates through every line of the file and then creates an ```INSERT INTO table_name (columns) VALUES(values);``` query for all of the records individually.

For a large dataset, this query might be time-taking as it creates an SQL syntax for all lines or rows in the files. If the data file and table is in same database server, we can use ```\COPY``` command of postgresql to import the CSV into the database. Since here, we have to append the country column into the database, we cannot use ```\COPY``` command.

The SQL syntax used for extracting _raw_category_ data is given below:
``` sql
INSERT INTO raw_category(id, title, kind, etag, channel_id, assignable, country)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(id)
DO UPDATE SET 
    country = EXCLUDED.country || ';' || raw_category.country;
```
Here, as many countries can have the same trending video, the countries are separated with a semicolon (;). 

Similarly, the archiving is done by the script below:
 ```python
     def archive_category_data(con, cur):
        with open("../sql/queries/extract_archive_raw_category_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query)       
                        con.commit()
        print("Archiving successful to archive_raw_category table.") 
 ```
 The SQL Query for archiving the _raw_category_ data is:
 ```sql
 INSERT INTO archive_raw_category(id, title, kind, etag, channel_id, assignable, country)
SELECT id, title, kind, etag, channel_id, assignable, country FROM raw_category
ON CONFLICT(id)
DO UPDATE SET
    country = EXCLUDED.country || ';' || archive_raw_category.country;
 ```
 
  For _raw_video_ table:
 ```python
   def extract_video_data(fileName, con, cur):
        country_list = fileName.split('.csv')[0]
        country = country_list[11:-6]
        print("Extracting " + country + " country data to raw_video table.") 
        print('...')
        with open(fileName,'r', encoding='ISO-8859-1') as f:
            i = 0
            csv_list = csv.reader(f)
            for row in csv_list:
                if i==0:
                    i+=1
                    continue
                row.append(country)
                with open("../sql/queries/extract_raw_video_data.sql") as f:
                    sql = ' '.join(map(str, f.readlines()))
                    cur.execute(sql, row)
                    con.commit()
        print("Extraction of "+country+" data successful to raw_video table.") 
 ```
 The data file for _raw_video_ table was in CSV format. We have to append country column here as well. Since the data set was large and not cleaned, there were false demiliters in the data. Hence, we used python's ```csv``` module to read csv data.
 
Another reason for iterating through every records of file is that, we can remove the header columns  which is present in the CSV file. In our database, the first row was the header of columns and it was removed using a counter ```i``` through our python script. 

The SQL Query for extraction of _raw_video_ table is:
```sql
 INSERT INTO raw_video (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,
                        dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description,country)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
```
Similarly, the python script and sql query for archiving _raw_video_ table are listed hereafter:

```python
 def archive_video_data(con, cur):
        with open("../sql/queries/extract_archive_raw_video_data.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            cur.execute(sql)
            con.commit()

        print("Archiving successful to copy_raw_video table.")
```

```sql
INSERT INTO archive_raw_video 
    (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,
    dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description,country)
SELECT 
    video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,
    comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description,country
FROM raw_video;

```
 The methods previously defined are called in the main function of python.
 
 ```python
   def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_customer", con, cur)
        
        extract_customer_data("../../data/customer_dump.csv",con,cur)
        archive_customer_data(con, cur)
      
        cur.close()
        con.close()

    if __name__ == '__main__':
        main()
 ```
 Here, we truncate the table before applying extraction so that no previous records are present in the raw table and the ETL process is completed smoothly.
 
 The similar process is used in extracting raw tables and archiving them for all of the other raw tables. Their code snippets are attached hereafter.
 

 All the SQL queries used in the extraction process are stored in a separate file and called from file reading lines that makes our extraction code clean and easier to debug.
 
 The SQL queries used in above extraction and throughout other process of transformation and loading are [here]().
 
## T - Transformation and L- Loading

After the extraction and archiving process is completed successfully, we have two raw tables and archive tables that stores raw data and hostoric raw datas respectively. 

In the transformation process, we can create separate tables or temporary tables to store values that is finally loaded into the final table of the warehouse i.e. facts and dimensions.

We create a category table to store category related information from the _raw_category_ table. This table will be used after to load Data Warehouse Tables.

```sql
INSERT INTO category(client_category_id, category_title, assignable)
SELECT
id as client_category_id,
title as category_title,
CAST(assignable as BOOLEAN)
FROM raw_category;
```
Since in our data, we do not need to create those intermediate tables, we directly apply transformation and load to the final tables of the warehouse. 

Each process of transforming and loading to the final tables are briefly described below:

#### ```dim_category``` table
In _dim_category_ table, we load it directly as it is a dimension table from the category table previously formed. The SQL query to apply transformation and load _dim_category_ table is:

```sql
INSERT INTO dim_category (category_id, category_title, assignable)
SELECT client_category_id, category, assignable FROM category;
```

####  In this way, we have successfully completed the Extraction, Transformation and Loading of raw YouTube Trending Video data into designed warehouse.

Some of the things to mention:
- All the python scripts were written with proper error handling using ```try-except``` block.
```python
from utils import *

try:
    #extraction methods here
    def main():
        con = connect()
        cur = con.cursor()

      #calling functions here

        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))
```
- The frequent queries were separated from the files to avoid repeating of code. ```connect()``` and ```truncate_table()``` are separated and placed as a different script named ```utils.py``` and imported whenever needed.
```python
import psycopg2
from dotenv import load_dotenv
load_dotenv()
import os

def connect():
        return psycopg2.connect(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("database")
        )

def truncate_table(table_name, con, cur):
    with open("../sql/queries/truncate_table.sql") as f:
        sql = ' '.join(map(str, f.readlines()))% table_name
        print(sql)
        
        cur.execute(sql)       
        con.commit()
```
 And lastly, dotenv was used for secret and confidential values as storing database credentials and was extracted from the file when needed.

