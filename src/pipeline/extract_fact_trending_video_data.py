from utils import *

try:
    
    def load_fact_trending_video_table(con, cur):
      with open("../sql/queries/load/load_fact_trending_video_data.sql") as file:
                      insert_query = ' '.join(map(str, file.readlines())) 
                      cur.execute(insert_query)       
                      con.commit()
      print("Loading successful to fact_trending_video table.") 
    
  
    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("fact_trending_video", con, cur)

        load_fact_trending_video_table(con, cur)
       
        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

