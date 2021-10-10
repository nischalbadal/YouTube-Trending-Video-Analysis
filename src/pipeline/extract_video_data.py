from utils import *
import csv

try:
    
   
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
                with open("../sql/queries/extract/extract_raw_video_data.sql") as f:
                    sql = ' '.join(map(str, f.readlines()))
                    cur.execute(sql, row)
                    con.commit()
        print("Extraction of "+country+" data successful to raw_video table.") 
    
       
    def archive_video_data(con, cur):
        with open("../sql/queries/extract/extract_archive_raw_video_data.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            cur.execute(sql)
            con.commit()

        print("Archiving successful to copy_raw_video table.") 

    def load_dim_channel_data_table(con, cur):
      truncate_table("dim_channel", con, cur)
      with open("../sql/queries/load/load_dim_channel_data.sql") as file:
                      insert_query = ' '.join(map(str, file.readlines())) 
                      cur.execute(insert_query)       
                      con.commit()
      print("Loading successful to dim_channel table.") 
    
    def load_dim_date_data_table(con, cur):
        truncate_table("dim_date", con, cur)
        with open("../sql/queries/load/load_dim_date_data.sql") as file:
                        insert_query = ' '.join(map(str, file.readlines())) 
                        cur.execute(insert_query)       
                        con.commit()
        print("Loading successful to dim_date table.") 

    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_video", con, cur)

        extract_video_data("../../data/CAvideos.csv",con,cur)
        extract_video_data("../../data/DEvideos.csv",con,cur)
        extract_video_data("../../data/FRvideos.csv",con,cur)
        extract_video_data("../../data/GBvideos.csv",con,cur)
        extract_video_data("../../data/INvideos.csv",con,cur)
        extract_video_data("../../data/JPvideos.csv",con,cur)
        extract_video_data("../../data/KRvideos.csv",con,cur)
        extract_video_data("../../data/MXvideos.csv",con,cur)
        extract_video_data("../../data/RUvideos.csv",con,cur)
        extract_video_data("../../data/USvideos.csv",con,cur)

        archive_video_data(con, cur)
        load_dim_channel_data_table(con, cur)
        load_dim_date_data_table(con, cur)
        
        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

