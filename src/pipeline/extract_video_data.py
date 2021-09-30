from utils import *

try:
   
    def extract_video_data(fileName, con, cur):
        with open(fileName, 'r', encoding='utf-8') as f:
            cur.copy_from(f, 'raw_video', columns=('video_id','trending_date','title','channel_title','category_id','publish_time','tags','views','likes','dislikes','comment_count','thumbnail_link','comments_disabled','ratings_disabled','video_error_or_removed','description'), sep=',')
        # with open("../sql/queries/extract_raw_video_data.sql") as f:
        #     sql = ' '.join(map(str, f.readlines()))%fileName
        #     print(sql)
        #     cur.execute(sql, fileName)     
        #     con.commit()

        print("Extraction successful to raw_video table.") 
    
       
    def archive_video_data(fileName,con, cur):
        with open("../sql/queries/extract_copy_raw_video_data.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            cur.execute(sql)
            con.commit()

        print("Archiving successful to copy_raw_video table.") 


    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_video", con, cur)

        extract_video_data("../../data/CAvideos.csv",con,cur)
        # archive_video_data(con, cur)

        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

