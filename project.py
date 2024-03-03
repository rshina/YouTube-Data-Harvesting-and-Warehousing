#capstone project ** YouTube Data Harvesting and Warehousing**

from pprint import pprint
import pandas as pd
import pymongo
import pymysql
import streamlit as st
import googleapiclient.discovery


#channel id list used for youtube Data Harvesting
ch_id_list=["UCNhaliLwhGH9wX3pe9bFTbA", 
"UCIJPUfUJWCrbgM1tfWHk63A",
"UCi3o8sgPl4-Yt501ShuiEgA",
"UCZN9jOlj8Dm1nYidAGJuA-w",
"UCZAyzJefV9LFIIqRe7pe8Yw",
"UCszuS_Rnu6qrynAx8RapcmA",
"UCKWaEZ-_VweaEx1j62do_vQ",
"UCyVH4H2qhHwmuPFQ5HMPgqw",
"UCteRPiisgIoHtMgqHegpWAQ",
"UCInztSxqsC52TZPqmQiX9GA"]
  
      
##1st step...youtube data Harvesting

#assign API key to a variable

api_key='AIzaSyD6XMtnxDoT_l6iupUYwSsAs6xPbNQlGSI'   
api_service_name="youtube"
api_version="v3"


 #connect to data api using api key
youtube = googleapiclient.discovery.build(                     
        api_service_name, api_version, developerKey=api_key)


#define a function To fetch channel informations                   
def fetch_channel_information(channel_id):
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id)
            response = request.execute()
            for items in response:
                        channel_information={'channel_id':response['items'][0]['id'],
                            'channel_name':response['items'][0]['snippet']['title'],
                             'channel_description':response['items'][0]['snippet']['description'],
                             'channel_subscriberCount':response['items'][0]['statistics']['subscriberCount'],
                              'channel_view_count':response['items'][0]['statistics']['viewCount'],
                               'playlist_id':response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}
            return channel_information

channel_informations=fetch_channel_information("UCInztSxqsC52TZPqmQiX9GA")


#define a function To fetch playlist information
def fetch_playlist_information(channel_id):
            next_page_token=None
            list_playlist=[]
            while True:
                response_playlist = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,maxResults=50,pageToken=next_page_token).execute()

                for item in response_playlist["items"]:
                        playlist_information={"playlist_id":item["id"],
                                            "playlist_name":item["snippet"]["title"],
                                             "channel_id":item["snippet"]["channelId"]}
                        list_playlist.append(playlist_information)
                next_page_token=response_playlist.get("nextPageToken")
                if next_page_token is None:
                            break
            return list_playlist
playlist_informations=fetch_playlist_information("UCInztSxqsC52TZPqmQiX9GA")



#define a function To fetch vedio informations
    
def fetch_vedio_information(channel_id):
        list_vedio_id=[]
        vedios_information=[]
                    #getting playlist_id from channel_information
        response_ = youtube.channels().list(
        part='contentDetails',id=channel_id).execute()                           




        playlist_id=response_['items'][0]['contentDetails']['relatedPlaylists']['uploads']

                    #2--fetch vedio_id play_list_id
        next=None
        while True:

                response_playlist= youtube.playlistItems().list(part='snippet',playlistId=playlist_id,maxResults=50,pageToken=next).execute()
                for item in range(len(response_playlist['items'])):
                                         list_vedio_id.append(response_playlist['items'][item]['snippet']['resourceId']['videoId'])
                next=response_playlist.get("nextPageToken")
                if next is None:
                     break




            #fetch vedio information using vedio_id list               
        vedios_information=[]       
        for i in list_vedio_id:
                    video_response = youtube.videos().list(
                            part='snippet,statistics,contentDetails',id=i).execute()
                    if video_response['items'][0]:
                        video_information = {"Video_Id": i,
                                            "Video_Name":video_response['items'][0]['snippet']['title'] if 'title' in video_response['items'][0]['snippet'] else "Not Available",
                                            "Video_Description":video_response['items'][0]['snippet']['description'],  
                                             "Tags":video_response['items'][0]['snippet']['tags']if 'tags' in video_response['items'][0]['snippet'] else "Not Available",
                                                "PublishedAt":video_response['items'][0]['snippet']['publishedAt'],
                                                "View_Count":video_response['items'][0]["statistics"]["viewCount"],
                                                "Like_Count":video_response['items'][0]["statistics"]["likeCount"]if 'likeCount' in video_response['items'][0]["statistics"] else "Not Available",
                                                "Favorite_Count":video_response['items'][0]["statistics"]["favoriteCount"],
                                                "Comment_Count":video_response['items'][0]["statistics"]["commentCount"]if "commentCount" in video_response['items'][0]["statistics"]else "Not Available",
                                                "Duration":video_response['items'][0]["contentDetails"]["duration"],
                                                "Thumbnail":video_response['items'][0]['snippet']["thumbnails"],
                                                "Caption_Status":video_response['items'][0]["contentDetails"]["caption"],
                                                 "channel_id":video_response['items'][0]['snippet']["channelId"]}
                    vedios_information.append( video_information)
        return vedios_information,list_vedio_id


vedios_informations,list_vedio_ids=fetch_vedio_information("UCInztSxqsC52TZPqmQiX9GA")



#define a function To fetch comment information
def fetch_comment_information(list_vedios_ids):
        comment__informations=[]
        try:
            for vedio__id in list_vedios_ids:
                         response_comment= youtube.commentThreads().list(
                            part="snippet",
                            videoId=vedio__id,maxResults=50).execute()


                         for item in response_comment['items'] : 
                                                 comment_information={"Comment": {"Comment_Id":item['id'],
                                                 "Comment_Text":item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                                "Comment_Author":item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                                  "Comment_PublishedAt":item['snippet']['topLevelComment']['snippet']['publishedAt'],
                                                 "Video_Id":item['snippet']['topLevelComment']['snippet']['videoId']}}

                                                 comment__informations.append(comment_information)
            
        except:
            pass
        return comment__informations


comment_informations=fetch_comment_information(list_vedio_ids) 

#Now i have all the data(channel data,playlist data,vedio data,comment data) of channels fetched from youtube data api


#step2........youtube data warehousing 
# To Convert all the accessed data into a single dictionary data
channel_1_information={"channel_information1":channel_informations,"playlist_information1":playlist_informations,"vedio_information1":vedios_informations,
                      "comment_infrmation1":comment_informations}



#To  tranfer and store the data into mongodb  using pymongo module                    
#connecting to MONGODB

def to_mongodb(channel_1_informations):
    client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
    mydb = client["project"]
    mycoll=mydb["project_mongodb"]
    #mycoll.insert_one(channel_1_informations)
    #return "data exported into mongodb"
channel_info=to_mongodb(channel_1_information)

#now stored all the informations in a single collection(project_mongodb)of mongodb
    
#SQL table creation by accesing data from MONGODB
#1)channel table creation

#TO connect MySQL
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='admin@123')
cur = myconnection.cursor()
#cur.execute("create database projectsql")
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='admin@123',database = "projectsql")
cur = myconnection.cursor()


#channel tables creation
def tables_sql():
    
        cur.execute("create table if not exists channel(channel_id varchar(50) primary key,channel_name varchar(255),channel_description text,channel_subscriberCount int,channel_view_count int,playlist_id varchar(50))")
        #playlist table creation

        cur.execute("create table if not exists playlist(playlist_id varchar(100) primary key,playlist_name varchar(255),channel_id varchar(50),FOREIGN KEY (channel_id) REFERENCES channel(channel_id))")
        #vedio table creation

        cur.execute("create table if not exists vedio(Video_Id varchar(100) primary key,Video_Name varchar(255),Video_Description text,Tags text,PublishedAt varchar(50) ,View_Count int,Like_Count int,Favorite_Count int ,Comment_Count int,Duration varchar(50),Thumbnail varchar(255),Caption_Status varchar(255),channel_id varchar(50))")
        #comment table creation
        
        cur.execute("create table if not exists comment(Comment_Id varchar(100) primary key,Comment_Text text,Comment_Author varchar(100),Comment_PublishedAt varchar(50),Video_Id varchar(100),FOREIGN KEY (Video_Id) REFERENCES vedio(Video_Id))")           
        return 'tables created'
#Tables=tables_sql())
 #now created 4 tables in sql 

#To insert values into tables(channel,playlist,vedio,comment )
def insert_value():
#insert channel records
            client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
            mydb = client["project"]
            mycoll=mydb["project_mongodb"]
 #convert mongodb records into dataframe
            channel_data=[]
            for item in mycoll.find({},{"_id":0,"channel_information1":1}):
                                                    channel_data.append(item['channel_information1'])
            df_channel=pd.DataFrame(channel_data)
#insert  into my_sql
            for index,row in df_channel.iterrows():
                                    insert='''insert into channel(channel_id,channel_name,channel_description,
                                                                                        channel_subscriberCount,channel_view_count,playlist_id)  
                                                                                        values(%s,%s,%s,%s,%s,%s)'''
                                    values=(row['channel_id'],
                                                        row['channel_name'],
                                                        row['channel_description'],
                                                        row['channel_subscriberCount'],
                                                        row['channel_view_count'],
                                                        row['playlist_id'])
                                    try:
                                        cur.execute(insert,values)
                                        myconnection.commit()
                                    except:
                                        print("values inserted")

#insert playlist records                                

            playlist_data=[]
            for item in mycoll.find({},{"_id":0,"playlist_information1":1}):
                                            for i in range (len(item["playlist_information1"])):
                                                         playlist_data.append(item["playlist_information1"][i])
                        
            
#convert mongodb records into dataframe      
            df_playlist=pd.DataFrame(playlist_data)

            for index,row in df_playlist.iterrows():
                                       insert1='''insert into playlist(playlist_id,playlist_name,channel_id)  
                                                                                values(%s,%s,%s)'''
                                       values1=(row['playlist_id'],
                                                            row['playlist_name'],
                                                            row['channel_id'])

                                       try:
                                            cur.execute(insert1,values1)
                                            myconnection.commit()
                                       except Exception as e:
                                                        print("values inserted",e)

#insert vedio records 
            vedio_data=[]
            for item in mycoll.find({},{"_id":0,"vedio_information1":1}):
                            for i in range (len(item["vedio_information1"])):
                                             vedio_data.append(item["vedio_information1"][i])
                        
#convert mongodb records into dataframe                         
            df_vedio=pd.DataFrame(vedio_data) 

            for index,row in df_vedio.iterrows():
                                    insert2='''insert into vedio(Video_Id,Video_Name,Video_Description,
                                           Tags,PublishedAt,View_Count,Like_Count,Favorite_Count,Comment_Count,
                                             Duration,Thumbnail,Caption_Status,channel_id)  
                                             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                    values2=(row['Video_Id'],
                                        row['Video_Name'],
                                        row['Video_Description'],
                                        row['Tags'],
                                        row['PublishedAt'],
                                        row['View_Count'],
                                        row['Like_Count'],
                                        row['Favorite_Count'],
                                        row['Comment_Count'],
                                        row['Duration'],
                                        row['Thumbnail']['default']['url'],
                                        row['Caption_Status'],
                                        row['channel_id'])




                                    try:
                                        cur.execute(insert2,values2)
                                        myconnection.commit()
                                    except Exception as e:
                                          print("values inserted",e)
#insert comment records


            comment_data=[]
            for item in mycoll.find({},{"_id":0,"comment_infrmation1":1}):
                                for i in range (len(item["comment_infrmation1"])):
                                    for j in (item["comment_infrmation1"][i]):
                                        comment_data.append(item["comment_infrmation1"][i][j])
                                    
#convert mongodb records into dataframe                                     
            df_comment=pd.DataFrame(comment_data)  
                        


            for index,row in df_comment.iterrows():
                                insert3='''insert into comment(Comment_Id,Comment_Text,Comment_Author,Comment_PublishedAt,Video_Id)  
                                                            values(%s,%s,%s,%s,%s)'''
                                values3=(row['Comment_Id'],
                                        row['Comment_Text'],
                                        row['Comment_Author'],
                                        row['Comment_PublishedAt'],
                                        row['Video_Id'])



                                try:
                                    cur.execute(insert3,values3)
                                    myconnection.commit()
                                except Exception as e:
                                      print("values inserted",e)

            return "records uplaoded"
#insert_values_tables=insert_value()
#Now Tables(CHANNEL,PLAYLIST,VEDIO,COMMENT) Created in sql


#Finally ,Have to Display the outputs in streamlit



#For streamlit Display

#for front view of streamlit

st.header(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]",divider='rainbow')
st.sidebar.caption("*Extract Data Using Google API")
st.sidebar.caption("*Store the Data into MONGODB") 
st.sidebar.caption("*Migrate tha Data to a SQL data warehouse")
st.sidebar.caption("*Data Management Using MongoDB,Pandas and SQL")
st.sidebar.caption("*Streamlit Display")
st.sidebar.caption("Used Tools::**Python**,**YouTube Data API** , **MONGODB** ,**MySQL**,**VSCode**,**Streamlit**")

        
 #1)
#To access channel information by giving channel-id(It will give all information about given channel_id)
input_channel_id=st.text_input("Enter a Youtube Channel ID(Comma saparated)")

submit_=st.button("Extract Data")
if submit_:
    def information_streamlit(Channel_ID):
          channel_1_information={"channel_information1":fetch_channel_information(Channel_ID),"playlist_information1":fetch_playlist_information(Channel_ID),"vedio_information1":fetch_vedio_information(Channel_ID),
                          "comment_infrmation1":fetch_comment_information(Channel_ID)}
           
          return channel_1_information
    channel_info=information_streamlit(input_channel_id)
    st.write(channel_info) 




#2))To collect and store the data into MONGODB,collect and store the information of channel_id gien by user

collect_store=st.button("Collect and store data")

if collect_store:
        if input_channel_id not in ch_id_list:

                def information_streamlit(Channel_ID1):
                        channel_1_information={"channel_information1":fetch_channel_information(Channel_ID1),"playlist_information1":fetch_playlist_information(Channel_ID1),"vedio_information1":fetch_vedio_information(Channel_ID1),
                                "comment_infrmation1":fetch_comment_information(Channel_ID1)}
                
                        client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
                        mydb = client["project"]
                        mycoll=mydb["project_mongodb"]
                        mycoll.insert_one(channel_1_information)
                        return "Data successfully stored in mongodb"
                R=information_streamlit(input_channel_id)
                st.success(R)
        else:
             st.error("Given channel Details already in MONGODB")






#To Migrate to sql          
if st.button("MIgrate Data to SQL and Create table"):
        tables_sql()
        if  input_channel_id not in ch_id_list:
                                  insert_value()
                                  st.success("Data Migration to sql is  Done")

 #Choose any channel and get information of spcific channel                                 
channel_names_list=[
"LifeofShazzam",	
"JASON MAKKI",
"Finally",
"MaanavaN Learn Code",
"Learn at Knowstar",
"Shaan Geo",
"IBM Technology",
"James Ernest", 
"A2D Bytes",
"TravelTriangle " ]   

user_input_ch_name = st.selectbox("Select channel",channel_names_list)
if user_input_ch_name :
        def channel_selct(user_input_ch_name1):
                   client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
                   mydb = client["project"]
                   mycoll=mydb["project_mongodb"]
                   channel__data=[]
                   for item in mycoll.find({},{"_id":0,"channel_information1":1}):
                                           if item['channel_information1']['channel_name']==user_input_ch_name1:
                                                                                 channel__data.append(item['channel_information1'])
                   return channel__data 
        result=channel_selct(user_input_ch_name)        

df1_channel1=st.dataframe(result)


                               
            
                        
             
#Query/Answer section
import seaborn as sns
import plotly_express as px
Query_Answer=st.selectbox("Select a Query",["1* What are the names of all vedios and their corresponding channels ?","2* Which channels have the most number of vedios,and how many vedios do thay have ?","3* What are the top 10 most viewed videos and their respective channels?",'4* How many comments were made on each video, and what are their corresponding video names ?',
                                         "5* Which videos have the highest number of likes, what are their corresponding channel names?","6* What is the total number of likes for each video, and what are their corresponding video names?","7* What is the total number of views for each channel, and what are their corresponding channel names?","8* What are the names of all the channels that have published videos in the year2022?",
                                         "9* What is the average duration of all videos in each channel, and what are their corresponding channel names?","10* Which videos have the highest number of comments, and what are their corresponding channel names?"])

if Query_Answer=="1* What are the names of all vedios and their corresponding channels ?":
        cur.execute("select channel.channel_name,vedio.video_name  from channel join vedio on vedio.channel_id = channel.channel_id")
        fechdata1=cur.fetchall()
        dataframe1=pd.DataFrame(fechdata1,columns=["CHANNEL_NAME","VEDIO_NAME"])
        st.write(dataframe1)
    
                                                                                 
elif Query_Answer=="2* Which channels have the most number of vedios,and how many vedios do thay have ?":
        cur.execute("select distinct channel_name,count(vedio.channel_id)as no_of_vedios from channel join vedio on vedio.channel_id = channel.channel_id group by vedio.channel_id order by no_of_vedios desc")
        fechdata2=cur.fetchall()
        dataframe2=pd.DataFrame(fechdata2,columns=["CHANNEL_NAME","NO_OF_VEDIOS"])
        st.write(dataframe2)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=dataframe2,x="CHANNEL_NAME",y="NO_OF_VEDIOS")

                                                                                    
elif Query_Answer=="3* What are the top 10 most viewed videos and their respective channels?":
        cur.execute("select channel.channel_name as channel_name,vedio.video_name,vedio.video_ID,vedio.view_count as view_count from channel  join vedio on vedio.channel_id = channel.channel_id group by vedio.video_ID order by vedio.view_count desc limit 10;")
        fechdata3=cur.fetchall()
        dataframe3=pd.DataFrame(fechdata3,columns=["CHANNEL_NAME","VEDIO_NAME","VEDIO_ID","VIEW_COUNT"])
        st.write(dataframe3)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=dataframe3,x="VEDIO_NAME",y="VIEW_COUNT")
                                                                              
                                                                                    
elif Query_Answer=="4* How many comments were made on each video, and what are their corresponding video names ?":
        cur.execute("select vedio.video_name as vedio_name ,count(comment.comment_id) as no_of_comment from vedio  join comment on vedio.video_id= comment.video_id group by  vedio.video_name")
        fechdata4=cur.fetchall()
        dataframe4=pd.DataFrame(fechdata4,columns=["VEDIO_NAME","NO_OF_COMMENTS"])
        st.write(dataframe4)
                                                                                    
elif Query_Answer=="5* Which videos have the highest number of likes, what are their corresponding channel names?":
        cur.execute("select channel.channel_name as channel_name ,vedio.video_name vedio_name,vedio.like_count as like_count from channel  join vedio on vedio.channel_id = channel.channel_id  order by like_count desc limit 10")
        fechdata5=cur.fetchall()
        dataframe5=pd.DataFrame(fechdata5,columns=["CHANNEL_NAME","VEDIO_NAME","LIKE_COUNT"])
        st.write(dataframe5)
        #For visualisation
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=dataframe5,x="VEDIO_NAME",y="LIKE_COUNT")
        


elif Query_Answer=="6* What is the total number of likes for each video, and what are their corresponding video names?":
        cur.execute("select video_name,like_count from vedio")
        fechdata6=cur.fetchall()
        dataframe6=pd.DataFrame(fechdata6,columns=["VEDIO_NAME","LIKE_COUNT"])
        st.write(dataframe6) 

elif Query_Answer=="7* What is the total number of views for each channel, and what are their corresponding channel names?":
        cur.execute("select channel_name,channel_view_count from channel")
        fechdata7=cur.fetchall()
        dataframe7=pd.DataFrame(fechdata7,columns=["CHANNEL_NAME","TOTAL_NUMBER_OF_VIEWS"]) 
        st.write(dataframe7)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=dataframe7,x="CHANNEL_NAME",y="TOTAL_NUMBER_OF_VIEWS")
       
        

elif Query_Answer=="8* What are the names of all the channels that have published videos in the year2022?":
        cur.execute("select channel.channel_name as channel_name ,vedio.publishedAt as publishedAt from channel  join vedio on vedio.channel_id = channel.channel_id where publishedAt=2022")
        fechdata8=cur.fetchall()
        dataframe8=pd.DataFrame(fechdata8,columns=["CHANNEL_NAME","PUBLISHED_DATE"])
        st.write(dataframe8)


elif Query_Answer=="9* What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        cur.execute("select channel.channel_name ,duration from channel  join vedio on vedio.channel_id = channel.channel_id")
        fechdata1=cur.fetchall()
        dataframe9=pd.DataFrame(fechdata1,columns=["channel_name",'average_duration'])
        list=dataframe9["average_duration"].to_list()
        list_=[]
        for duration_str in list:
       
        
                duration_str = duration_str.replace("PT", "")
                                        
                                        # Initialize hours, minutes, and seconds to 0
                hours, minutes, seconds = 0, 0, 0
                                        
                                        # Parse the duration string to extract hours, minutes, and seconds
                if 'H' in duration_str:
                                        hours_str, duration_str = duration_str.split('H')
                                        hours = int(hours_str)
                if 'M' in duration_str:
                                        minutes_str, duration_str = duration_str.split('M')
                                        minutes = int(minutes_str)
                if 'S' in duration_str:
                                        seconds_str = duration_str.replace('S', '')
                                        seconds = int(seconds_str)
                                        
                                        # Format the duration as HH:MM:SS
                formatted_duration = f"{hours:02}:{minutes:02}:{seconds:02}"
                list_.append(formatted_duration)
        dataframe9["average_duration"]=list_
        dataframe9['average_duration'] = pd.to_timedelta(dataframe9['average_duration'])
        DATA=dataframe9.groupby('channel_name')['average_duration'].mean() 
        New_dataframe=pd.DataFrame(DATA,columns=["average_duration"])   
        st.write(New_dataframe)


elif Query_Answer=="10* Which videos have the highest number of comments, and what are their corresponding channel names?":
        cur.execute("select channel.channel_name as channel_name ,vedio.video_name,vedio.comment_count from channel  join vedio on vedio.channel_id = channel.channel_id ORDER BY VEDIO.COMMENT_COUNT DESC LIMIT 10")
        fechdata10=cur.fetchall()
        dataframe10=pd.DataFrame(fechdata10,columns=["CHANNEL_NAME","VEDIO_NAME","COMMENT_COUNT"])
        st.write(dataframe10)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=dataframe10,x="CHANNEL_NAME",y="COMMENT_COUNT")
#Here completed the project YouTube Data Harvesting and Warehousing
        
 






    
