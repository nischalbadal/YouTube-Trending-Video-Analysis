from utils import *
import json
from psycopg2.extras import Json


try:
   
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
                    with open("../sql/queries/extract/extract_raw_category_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query, row)       
                        con.commit()
        print("Extraction of "+country+" data successful to raw_category table.") 
       
    def archive_category_data(con, cur):
        with open("../sql/queries/extract/extract_archive_raw_category_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query)       
                        con.commit()

        print("Archiving successful to archive_raw_category table.") 

    def load_dim_category_data_table(con, cur):
        truncate_table("dim_category", con, cur)
        with open("../sql/queries/load/load_dim_category_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query)       
                        con.commit()
        print("Loading successful to dim_category table.") 
    
    def load_dim_country_data_table(con, cur):
        truncate_table("dim_country", con, cur)
        with open("../sql/queries/load/load_dim_country_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query)       
                        con.commit()
        print("Loading successful to dim_country table.") 
      

    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_category", con, cur)
        truncate_table("archive_raw_category", con, cur)
        
        extract_category_data("../../data/CA_category_id.json",con,cur)
        extract_category_data("../../data/DE_category_id.json",con,cur)
        extract_category_data("../../data/FR_category_id.json",con,cur)
        extract_category_data("../../data/GB_category_id.json",con,cur)
        extract_category_data("../../data/IN_category_id.json",con,cur)
        extract_category_data("../../data/JP_category_id.json",con,cur)
        extract_category_data("../../data/KR_category_id.json",con,cur)
        extract_category_data("../../data/MX_category_id.json",con,cur)
        extract_category_data("../../data/RU_category_id.json",con,cur)
        extract_category_data("../../data/US_category_id.json",con,cur)

        archive_category_data(con,cur)
        load_dim_category_data_table(con, cur)
        load_dim_country_data_table(con, cur)
        
        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

