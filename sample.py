import streamlit as st
from streamlit_option_menu import option_menu
from pprint import pprint
import mysql.connector as sql
import pymongo
import pandas as pd
import seaborn as sns
from googleapiclient.discovery import build
st.set_page_config(page_title= "Youtube Analysis",
                   page_icon= '📈',
                   layout= "wide",)
api_key = "AIzaSyAuKH6NM2ZqEMNxSSBe1N3hQ4FOjVXaHV4"
youtube = build('youtube','v3',developerKey=api_key)


def fetch_channel_info(channel_id):
    request = youtube.channels().list(part ='snippet,contentDetails,statistics',id=channel_id
        
    )
    response = request.execute()
    for items in response:
        extracted_data={'channel_name':response['items'][0]['snippet']['title'],
                        'channel_id':response['items'][0]['id'],
                        'subscription_count':response['items'][0]['statistics']['subscriberCount'],
                        'channel_views':response['items'][0]['statistics']['viewCount'],
                        'channel_description':response['items'][0]['snippet']['description'],
                        'playlist_id':response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
        return extracted_data

#fetch playlistID
def fetchplaylist(channel_id):
    next_page_token=None

    while True:
        playlist1=youtube.playlists().list(part="snippet,contentDetails",channelId=channel_id,maxResults=50, pageToken=next_page_token).execute()
        playlist=[]
  
        for item in playlist1["items"]:
            playlistinfo={"playlistid":item["id"],
                      "playlistname":item["snippet"]["title"],
                      "channelid":item["snippet"]['channelId']}
            playlist.append(playlistinfo)
        next_page_token=playlist1.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist
#fetch videodetails
def fetchvideoid(channel_id):
    videoid=[]
    response=youtube.channels().list(part="contentDetails",id=channel_id).execute()
    playlistid=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
  
    while True:
       
            response1=youtube.playlistItems().list(part="snippet",playlistId=playlistid,maxResults=50,pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                videoid.append(response1["items"][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
            if next_page_token is None:
                 break
        
    videoinfo=[]
    for i in videoid:
            response2 = youtube.videos().list(part="snippet,contentDetails,statistics",id=i).execute()
            if response2['items'][0]:
                videodetails={"video_id":i,
                        "video_name":response2['items'][0]['snippet']['title'],
                        "video_description":response2["items"][0]["snippet"]["description"],
                        #"Tags":response2["items"][0]["snippet"].get("tags"),
                        "Published_at":response2["items"][0]["snippet"]["publishedAt"],
                        "View_count":response2["items"][0]["statistics"].get("viewCount","Not available"),
                        "Like_count":response2["items"][0]["statistics"].get("likeCount","Not available"), #if "likeCount" in response2["items"][0]["statistics"]["likeCount"] else "Not available",
                        "Dislike_count":response2['items'][0]['statistics'].get("dislikeCount","Not available"),
                        "Favorite_count":response2["items"][0]["statistics"].get("favoritecount","Not available"),
                        "comment_count":response2["items"][0]["statistics"].get("commentCount","Not available"), #if "commentCount" in response2["items"][0]["statistics"]["commentCount"] else "Not available",
                        "Duration":response2["items"][0]["contentDetails"]["duration"],
                        #"Thumbnails":response2['items'][0]['snippet']['thumbnails'],
                        "caption_status":response2["items"][0]["contentDetails"]["caption"],
                        "channel_id":response2["items"][0]["snippet"]["channelId"]}
            videoinfo.append(videodetails)
    return videoinfo,videoid

#fetch commentdetails
def fetchcommentdetails(videoid):
    comment=[]
    for video_id in videoid:
        try:
            response3 = youtube.commentThreads().list(part="snippet",videoId=video_id,maxResults=50).execute()
            for i in response3["items"]:
                commentinfo={"commentid":response3["items"][0]["id"],
                         "videoid":response3["items"][0]["snippet"]["videoId"],
                         "comment_text":response3["items"][0]["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                         "comment_author":response3["items"][0]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                         "comment_published_date":response3["items"][0]["snippet"]["topLevelComment"]["snippet"]["publishedAt"]}
            comment.append(commentinfo)
        except:
            pass
    return comment

client=pymongo.MongoClient('localhost:27017')
db=client["youtubeproject"]
col=db["project_mongodb"]
#storetomongodb
def entire_channeldata(channelid):
    channelinfo=fetch_channel_info(channelid)
    playlistid=fetchplaylist(channelid)
    videoinfo,videoid=fetchvideoid(channelid)
    comment=fetchcommentdetails(videoid) 
    client=pymongo.MongoClient('localhost:27017')
    db=client["youtubeproject"]
    col= db["project_mongodb"]
    channel_data={"channel_info":channelinfo,"playidslist":playlistid,"videoidlist1":videoinfo,"commentinfo":comment}
    channel_details=[]
    for item in col.find({},{"_id":0,"channel_info":1}):
        if "channel_info" in item:
            channel_details.append(item["channel_info"])
        else:
             print(f"Key 'channel_info' not found in item: {item}")
    #channel_details
    channel_name=[]
    for item2 in channel_details:
        channel_name.append(item2["channel_name"])
    #channel_name
    new_channel=channel_data["channel_info"]["channel_name"]
    if new_channel not in channel_name:
        col.insert_one(channel_data)
        print("upload completed successfully")
        return channel_data 
    else:
        print("data already saved")

#sql connection
import mysql.connector as sql
mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
cur=mydb.cursor()

#Table creation for channeldetails,playlistdetails,video details,comment details
def create_table():
    cur.execute("create table if not exists channelinfo1(channel_id varchar(100) primary key, channel_name varchar(100),subscription_count int, channel_views int,channel_description text,playlist_id varchar(200))")
    cur.execute("create table if not exists playlistinfo1(playlistid varchar(100) primary key,playlistname varchar(100),channelid varchar(100),FOREIGN KEY (channelid) REFERENCES channelinfo1(channel_id))")
    cur.execute("create table if not exists videolistinfo2(video_id varchar(100)primary key, Video_name varchar(200),video_description text,Published_at varchar(100),View_count int,Like_count varchar(100),Dislike_count varchar(100),Favorite_count int,comment_count int,Duration varchar(100),caption_status varchar(300),channel_id varchar(100))")
    cur.execute("create table if not exists comments1(commentid varchar(100)primary key,videoid varchar(100),comment_text text,comment_author varchar(200),comment_published_date varchar(100),FOREIGN KEY (videoid) REFERENCES videolistinfo2(video_id))")
    return "table created"

table1=create_table()

#insert channelvalues into the channelinfotable 
def channel_insert():
    client=pymongo.MongoClient('localhost:27017')
    db=client["youtubeproject"]
    col=db["project_mongodb"]
    channel_details=[]
    for item in col.find({},{"_id":0,"channel_info":1}):
         #if "channel_info" in item:
            channel_details.append(item["channel_info"])
    #print("Raw data from MongoDB:", channel_details)
    
    df = pd.DataFrame(channel_details)
    
    mydb = sql.connect(
            host="localhost",
            user="root",
            password="NyL@shr33",
            database="youtube2"
        )
    cur = mydb.cursor()
        
    for item, row in df.iterrows():
            insert_query = '''INSERT IGNORE INTO channelinfo1(channel_id, channel_name, subscription_count, channel_views, channel_description, playlist_id)
                              VALUES(%s, %s, %s, %s, %s, %s)'''
            values = (row['channel_id'], row['channel_name'], row['subscription_count'], row['channel_views'], row['channel_description'], row['playlist_id'])
            try:
                cur.execute(insert_query, values)
                mydb.commit()
                print("Insertion successful")
            except Exception as e:
                mydb.rollback()
                print(f"Error occurred: {e}")
                return f"Error occurred: {e}"
                

#insert playlistvalues into the playlistinfotable 
def playlist_insert():
    client=pymongo.MongoClient('localhost:27017')
    db=client["youtubeproject"]
    col= db["project_mongodb"]  
    playlist_details=[]
    for item in col.find({},{"_id":0,"playidslist":1}):
        for i in range(len(item["playidslist"])):
            playlist_details.append(item["playidslist"][i])
        df1=pd.DataFrame( playlist_details)
    
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor()
    
    for item,row in df1.iterrows():
            insert_query='''INSERT IGNORE INTO playlistinfo1(playlistid,playlistname,channelid)
                                values(%s,%s,%s)'''
            values=(row['playlistid'],
                    row['playlistname'],
                    row['channelid']
                    )
            try:
                cur.execute(insert_query,values)
                mydb.commit()
            except Exception as e:
                mydb.rollback() 
                return ("inserted successfully",e)
                

#insert videovalues into the videolistinfotable 
def video_insert():
    client=pymongo.MongoClient('localhost:27017')
    db=client["youtubeproject"]
    col= db["project_mongodb"]  
    video_details=[]
   
    for item in col.find({},{"_id":0,"videoidlist1":1}):
        for i in range(len(item["videoidlist1"])):
            video_details.append(item["videoidlist1"][i])
        df2=pd.DataFrame(video_details)
    
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor()
    
    for item,row in df2.iterrows():
                insert_query1='''INSERT IGNORE INTO  videolistinfo2(video_id,Video_name,video_description,Published_at,View_count,Like_count,DisLike_count,Favorite_count,comment_count,Duration,caption_status,channel_id)
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values1=(row['video_id'],
                        row['video_name'],
                        row['video_description'],
                        row['Published_at'],
                        row['View_count'],
                        row['Like_count'],
                        row['Dislike_count'],
                        row['Favorite_count'],
                        row['comment_count'],
                        row['Duration'],
                        row['caption_status'],
                        row['channel_id']
                         )
                try:
                    cur.execute(insert_query1,values1)
                    mydb.commit()
                
                except Exception as e:
                    mydb.rollback() 
                    return ("inserted successfully",e)
                    

#insert commentvalues into the commentstable 
def comment_insert():
    client=pymongo.MongoClient('localhost:27017')
    db=client["youtubeproject"]
    col= db["project_mongodb"]  
    comment_details=[]
    for item in col.find({},{"_id":0,"commentinfo":1}):
        for i in range(len(item["commentinfo"])):
            comment_details.append(item["commentinfo"][i])
        df3=pd.DataFrame(comment_details)
    
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor()
    
    for item, row in df3.iterrows():
            insert_query4='''INSERT IGNORE INTO comments1(commentid,videoid,comment_text,comment_author,comment_published_date)
                            values(%s,%s,%s,%s,%s)'''
            values4=(row["commentid"],
                    row["videoid"],
                    row["comment_text"],
                    row["comment_author"],
                    row["comment_published_date"])
            try:
                cur.execute(insert_query4,values4)
                mydb.commit()
            except Exception as e:
                mydb.rollback()
                return ("insert successfully",e)
                 
            
#define a table for called all functions by calling this table 
def table():
    
    channel_insert()
    playlist_insert()
    video_insert()
    comment_insert()
    return "Inserted successfully"
    

#tables=table()

#showtables

#show channeltable
def show_channels():
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor(dictionary=True)
    
    query="select * from channelinfo1"
    cur.execute(query)
    data=cur.fetchall()

    if data:
        st.dataframe(data)
    else:
        st.write("No data available in the CHANNELS table.")
    
#show playlisttable
def show_playlists():
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor(dictionary=True)
    query="select * from playlistinfo1"
    cur.execute(query)
    data=cur.fetchall()

    if data:
        st.dataframe(data)
    else:
        st.write("No data available in the PLAYLIST table.")

#show video table
def show_videos():
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor(dictionary=True)
    query="select * from videolistinfo2"
    cur.execute(query)
    data=cur.fetchall()

    if data:
        st.dataframe(data)
    else:
        st.write("No data available in the VIDEOS table.")
    
#show comment table
def show_comment():
    mydb=sql.connect(host="localhost",
                 user="root",
                 password="NyL@shr33",
                 database="youtube2"
                )
    cur=mydb.cursor(dictionary=True)
    
    query="select * from comments1"
    cur.execute(query)
    data=cur.fetchall()

    if data:
        st.dataframe(data)
    else:
        st.write("No data available in the COMMENTS table.")

#streamlit part -> To design the web app and visualize the channel details 
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd

#Visualization of process 
st.header(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
selected = option_menu(
    menu_title=None,
    options= ["Home", "Fetch and Save", "Show Tables", "Insights"],
    menu_icon=None,
    icons=None,
    orientation="horizontal"

    #styles={}
)

# About a project description using streamlit
if selected=="Home":
    st.title("About")
    st.text('''This project builds a userfriendly streamlit application that utilizes the Google API to extract information
            on a YouTube channel, stores it in a SQL database, and enables users to search for channel details and join tables 
            to view data in the Streamlit app.''')
    st.subheader("Technologies Expertise")
    st.caption("Streamlit")
    st.text('''A user-friendly UI built using Streamlit library, allowing users to interact with the application and perform data retrieval and analysis tasks.''')
    st.caption("YouTube API")
    st.text('''Integration with the YouTube API to fetch channel and video data based on the provided channel ID.''')
    st.caption("Python")
    st.text('''The programming language used for building the application and scripting tasks.''')
    st.caption("MongoDB")
    st.text('''Storage of the retrieved data in a MongoDB database, providing a flexible and scalable solution for storing unstructured and semi-structured data.''')
    st.caption("MySQL")
    st.text('''Migration of data from the data lake to a SQL database, allowing for efficient querying and analysis using SQL queries.''')
    st.caption("Visual Stdio Code")
    st.text('''Visual Studio Code is a code editor redefined and optimized for building and debugging modern web and cloud applications.''')

# Extract the channeldetails and stored into MONGODB
if selected=="Fetch and Save":
    input_channelid=st.text_input("Enter channel ID")
    submit=st.button("Extract and Save into MongoDB")   
    if submit:
        if input_channelid:
            client=pymongo.MongoClient('localhost:27017')
            db=client["youtubeproject"]
            col= db["project_mongodb"]
            ch_id=[]
            for ch_data in col.find({},{"_id":0,"channel_info":1}):
                ch_id.append(ch_data["channel_info"]["channel_id"])
            if input_channelid not in ch_id:
                insert=entire_channeldata(input_channelid)
                st.write(insert)
                st.success("Data saved into MongoDB")
            else:
                st.success("Data already exist")
    submit1=st.button("Migrate to sql")
    if submit1:
        migration_status = table()
        st.success(migration_status)

#Displaying the tables for all channels
if selected == "Show Tables":
    show_table = st.radio("Select the table to view", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))
    
    if show_table == "CHANNELS":
        df = show_channels()
        if df:
            st.dataframe(df)
       
    
    elif show_table == "PLAYLISTS":
        df = show_playlists()
        if df:
            st.dataframe(df)
        
    
    elif show_table == "VIDEOS":
        df = show_videos()
        if df:
            st.dataframe(df)
       
    
    elif show_table == "COMMENTS":
        df = show_comment()
        if df: 
            st.dataframe(df)
        
#Insights
if selected=="Insights":
    ques=st.selectbox("select the query",("1.What are the names of all the videos and their corresponding channels?",
                                           "2.Which channels have the most number of videos, and how many videos do they have?",
                                           "3.What are the top 10 most viewed videos and their respective channels?",
                                           "4.How many comments were made on each video, and what are their corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8.What are the names of all the channels that have published videos in the year 2022?",
                                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
    
    if ques=="1.What are the names of all the videos and their corresponding channels?":
        cur.execute("select channelinfo1.channel_name,videolistinfo2.video_name from channelinfo1 join videolistinfo2 on videolistinfo2.channel_id=channelinfo1.channel_id")
        fetchdata=cur.fetchall()
        df=pd.DataFrame(fetchdata,columns=["CHANNEL_NAME","VIDEO_NAME"])
        st.write(df)
        
    elif ques=="2.Which channels have the most number of videos, and how many videos do they have?":
        cur.execute("select distinct c.channel_name,count(v.channel_id) as No_of_videos from channelinfo1 as c join videolistinfo2 as v on v.channel_id=c.channel_id group by v.channel_id order by No_of_videos desc")
        fetchdata1=cur.fetchall()
        df1=pd.DataFrame(fetchdata1,columns=["CHANNEL_NAME","NO_OF_VIDEOS"])
        st.write(df1)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=df1,x="CHANNEL_NAME",y="NO_OF_VIDEOS")
    elif ques=="3.What are the top 10 most viewed videos and their respective channels?":
        cur.execute("select c.channel_name,v.video_id,v.Video_name, v.view_count as View_count from channelinfo1 as c join videolistinfo2 as v on c.channel_id=v.channel_id group by v.video_id order by View_count desc limit 10")
        fetchdata2=cur.fetchall()
        df2=pd.DataFrame(fetchdata2,columns=["CHANNEL_NAME","VIDEO_ID","VIDEO_NAME","VIEW_COUNT"])
        st.write(df2)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=df2,x="CHANNEL_NAME",y="VIEW_COUNT")
    elif ques=="4.How many comments were made on each video, and what are their corresponding video names?":
        cur.execute("select Video_name,comment_count  from videolistinfo2")
        fetchdata3=cur.fetchall()
        df3=pd.DataFrame(fetchdata3,columns=["VIDEO_NAME","NO_OF_COMMENTS"])
        st.write(df3)
        
    elif ques=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        cur.execute("select c.channel_name,v.Video_name,v.Like_count as Top_likecounts from channelinfo1 as c join videolistinfo2 as v on c.channel_id=v.channel_id order by Top_likecounts desc limit 10")
        fetchdata4=cur.fetchall()
        df4=pd.DataFrame(fetchdata4,columns=["CHANNEL_NAME","VIDEO_NAME","TOP_LIKECOUNTS"])
        st.write(df4)
        
    elif ques=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        cur.execute("select Video_name,Like_count,Dislike_count from videolistinfo2")
        fetchdata5=cur.fetchall()
        df5=pd.DataFrame(fetchdata5,columns=["VIDEO_NAME","LIKE_COUNT","DISLIKE_COUNT"])
        st.write(df5)
    elif ques=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        cur.execute("select  channel_name,channel_views from channelinfo1")
        fetchdata6=cur.fetchall()
        df6=pd.DataFrame(fetchdata6,columns=["CHANNEL_NAME","CHANNEL_VIEWS"])
        st.write(df6)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=df6,x="CHANNEL_NAME",y="CHANNEL_VIEWS")
    elif ques=="8.What are the names of all the channels that have published videos in the year 2022?":
        cur.execute("select c.channel_name,v.Published_at from channelinfo1 as c join videolistinfo2 as v on c.channel_id=v.channel_id where Published_at=2022")
        fetchdata7=cur.fetchall()
        df7=pd.DataFrame(fetchdata7,columns=["CHANNEL_NAME","PUBLISHED_AT"])
        st.write(df7)
        
    elif ques=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        cur.execute("""select c.channel_name,AVG(IFNULL(
            SUBSTRING_INDEX(SUBSTRING_INDEX(v.Duration, 'PT', -1), 'S', 1) +
            SUBSTRING_INDEX(SUBSTRING_INDEX(v.Duration, 'PT', -1), 'M', 1) * 60 +
            SUBSTRING_INDEX(SUBSTRING_INDEX(v.Duration, 'PT', -1), 'H', 1) * 3600,0)
            ) AS Average_Duration_Seconds from channelinfo1 as c join videolistinfo2 as v on c.channel_id=v.channel_id group by c.channel_name""")
        fetchdata8=cur.fetchall()
        df8=pd.DataFrame(fetchdata8,columns=["CHANNEL_NAME","DURATION"])
        st.write(df8)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=df8,x="CHANNEL_NAME",y="DURATION")

    elif ques=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        cur.execute("select c.channel_name,v.video_name,v.comment_count from channelinfo1 as c join videolistinfo2 as v on c.channel_id=v.channel_id order by comment_count desc")
        fetchdata9=cur.fetchall()
        df9=pd.DataFrame(fetchdata9,columns=["CHANNEL_NAME","VIDEO_NAME","COMMENT_COUNT"])
        st.write(df9)
        sns.set(rc={'figure.figsize':(18,4)})
        st.bar_chart(data=df9,x="CHANNEL_NAME",y="COMMENT_COUNT")
            


       