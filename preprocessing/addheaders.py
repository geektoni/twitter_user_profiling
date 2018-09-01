<<<<<<< HEAD
import sys
import io

fp = open('/mnt/c/Users/Annalisa/Documents/Dataset/data_02/text_20.csv', 'r')
fn = open('/mnt/c/Users/Annalisa/Documents/Dataset/data_02/text_20_wh.csv', 'w') 

headers='tweet_id,user_id,text,geo_lat,geo_long,place,place_id\n'
fn.write(headers)
fn.writelines(fp)
fn.close
fp.close
=======
import sys
import io

fp = open('/mnt/c/Users/Annalisa/Documents/Dataset/data_02/text_20.csv', 'r')
fn = open('/mnt/c/Users/Annalisa/Documents/Dataset/data_02/text_20_wh.csv', 'w') 

headers='tweet_id,user_id,text,geo_lat,geo_long,place,place_id\n'
fn.write(headers)
fn.writelines(fp)
fn.close
fp.close
>>>>>>> c6d22a7e58d67bb13338c96739aac2f015b2646c
