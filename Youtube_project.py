import streamlit as st
import googleapiclient.discovery
import pandas as pd
import mysql.connector
import time



#API Connection 

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCxOKrk7dJSJGu-ulyE1PIw4dHdKChFzug"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)




#get channels information
def channel_info(channel_id): 
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()

    if 'items' not in response or len(response['items']) == 0:
        st.error("No channel found with the provided ID.")
        return

    channel_data = response['items'][0]
    data = [{
        'channel_name': channel_data['snippet']['title'],
        'channel_des': channel_data['snippet']['description'],
        'channel_uploadId': channel_data['contentDetails']['relatedPlaylists']['uploads'],
        'channel_sub': channel_data['statistics']['subscriberCount'],
        'channel_vedioCount': channel_data['statistics']['videoCount'],
        'channel_viewCount': channel_data['statistics']['viewCount']
    }]

    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#get_playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


#upload to mongoDB

client=pymongo.MongoClient("mongodb+srv://naveen:naveend@naveen.vrsd7ro.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"




#SQL Connection

mydb = mysql.connector.connect(
 host="localhost",
 user="root",
 password="",
 )


cursor = mydb.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS Youtube")
cursor.execute("use Youtube")
    


def create_channels_table():
    create_query = ('''create table if not exists channels (
                       channel_name varchar(100),
                       channel_id varchar(100) primary key,
                       subscribers bigint,
                       views bigint,
                       total_videos int,
                       channel_description text,
                       playlist_id varchar(100)
                       )'''
        )

    cursor.execute(create_query)
    mydb.commit()

    ch_list = []
    for ch_data in collection.find({}, {"_id" : 0, "channel_details": 1}):
        ch_list.append(ch_data['channel_details'])
        
    df = pd.DataFrame(ch_list)
    
    for index, row in df.iterrows():
        insert_query = '''insert into channels(
                            channel_name,
                            channel_id,
                            subscribers,
                            views,
                            total_videos,
                            channel_description,
                            playlist_id
                            )
                            values(%s, %s, %s, %s, %s, %s, %s)'''

        values = (row['channel_name'],
                 row['channel_id'],
                 row['subscribers'],
                 row['views'],
                 row['total_videos'],
                 row['channel_description'],
                 row['playlist_id'])

        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")

def create_playlist_table():
    create_query = ('''create table if not exists playlists (
                               playlist_id varchar(255) primary key,
                               channel_id varchar(255),
                               playlist_name varchar(255),
                               video_count int,
                               FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
                               )
                               '''
                )

    cursor.execute(create_query)
    mydb.commit()
    
    pl_list = []
    for pl_data in collection.find({}, {"_id" : 0, "playlist_details": 1}):
        for i in range(len(pl_data['playlist_details'])):
            pl_list.append(pl_data['playlist_details'][i])

    df_playlist = pd.DataFrame(pl_list)
    
    for index, row in df_playlist.iterrows():    
        insert_query = '''insert into playlists(
                            playlist_id,
                            channel_id,
                            playlist_name,
                            video_count
                            )
                            values(%s, %s, %s, %s)'''

        values = (
                 row['playlist_id'],
                 row['channel_id'],
                 row['playlist_name'],
                 row['video_count']
                 )
        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")


def create_video_table():
    try:
        create_query = ('''create table if not exists videos (
                           video_id varchar(255) primary key,
                           channel_id varchar(255),
                           video_name varchar(255),
                           published_date varchar(255),
                           view_count int,
                           like_count int,
                           favourite_count int,
                           comment_count int,
                           duration varchar(100),
                           thumbnail varchar(255),
                           caption_status varchar(255)
                           )
                           '''
            )

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Table already created")
        
    vi_list = []
    for vi_data in collection.find({}, {"_id" : 0, "video_details": 1}):
        for i in range (len(vi_data['video_details'])):
            vi_list.append(vi_data['video_details'][i])

    df_video = pd.DataFrame(vi_list)
    
    for index, row in df_video.iterrows():    
        insert_query = '''insert into videos(
                            video_id,
                            channel_id,
                            video_name,
                            published_date,
                            view_count,
                            like_count,
                            favourite_count,
                            comment_count,
                            duration,
                            thumbnail,
                            caption_status
                            )
                            values(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (
                 row['video_id'],
                 row['channel_id'],
                 row['video_name'],
                 row['published_date'],
                 row['view_count'],
                 row['like_count'],
                 row['favourite_count'],
                 row['comment_count'],
                 row['duration'],
                 row['thumbnail'],
                 row['caption_status'],
                 )

        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")


def create_comment_table():
    try:
        create_query = ('''create table if not exists comments (
                           comment_id varchar(255) primary key,
                           video_id varchar(255),
                           comment_text text,
                           comment_author varchar(255),
                           comment_published_date varchar(255),
                           FOREIGN KEY (video_id) REFERENCES videos(video_id)
                           )
                           '''
            )

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Table already created")
        
    cm_list = []
    
    for cm_data in collection.find({}, {"_id" : 0, "comments_details": 1}):
        for i in range (len(cm_data['comments_details'])):
            cm_list.append(cm_data['comments_details'][i])

    df_comments = pd.DataFrame(cm_list)
    
    for index, row in df_comments.iterrows():    
        insert_query = '''insert into comments(
                            comment_id,
                            video_id,
                            comment_text,
                            comment_author,
                            comment_published_date
                            )
                            values(%s, %s, %s, %s, %s)'''

        values = (
                 row['comment_id'],
                 row['video_id'],
                 row['comment_text'],
                 row['comment_author'],
                 row['comment_published_date'],
                 )

        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")

def create_tables():
    create_channels_table()
    create_playlist_table()
    create_video_table()
    create_comment_table()
    
    return "Tables Created Successfully"


def get_channel_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']

    ch_list = []
    for ch_data in collection.find({}, {"_id" : 0, "channel_details": 1}):
        ch_list.append(ch_data['channel_details'])

    df1 = st.dataframe(ch_list)
    


def get_playlist_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']
    
    pl_list = []
    for pl_data in collection.find({}, {"_id" : 0, "playlist_details": 1}):
        for i in range(len(pl_data['playlist_details'])):
            pl_list.append(pl_data['playlist_details'][i])

    df2 = st.dataframe(pl_list)



def get_comment_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']

    cm_list = []
    for cm_data in collection.find({}, {"_id" : 0, "comments_details": 1}):
        for i in range (len(cm_data['comments_details'])):
            cm_list.append(cm_data['comments_details'][i])

    df3 = st.dataframe(cm_list)


def get_video_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']
    
    vi_list = []
    for vi_data in collection.find({}, {"_id" : 0, "video_details": 1}):
        for i in range (len(vi_data['video_details'])):
            vi_list.append(vi_data['video_details'][i])

    df4 = st.dataframe(vi_list)



##  Streamlit App Sidebar Configuration
with st.sidebar:
    st.title(':blue[YouTube Data Harvesting and Warehousing]')
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('API Integration')
    st.caption('Data Management using Pandas and SQL')

##  Main Page Title
st.title(':blue[YOUTUBE DATA HARVESTING & WAREHOUSING]')


## Data Extraction Header
st.header(':blue[YouTube Data Extraction]')


##  User Input - Channel ID through Streamlit App
channel_id=st.text_input('Enter the Channel ID')




#                                                       DATA DISPLAY
## Data Transformation Header
st.header(':blue[YouTube Data Display - After Transformation]')


##  Display the data extracted for the input channel ID
if st.button('Channel'):
    st.subheader('Channel Data Table')
    channel_df
if st.button('Videos'):
    st.subheader('Video Data Table')
    video_df
if st.button('Comments'):
    st.subheader('Comment Data Table')
    comment_df



#                                                       LOAD DATA INTO SQL DB
## Data Loading Header
st.header(':blue[YouTube Data Loading]')


##  Load the youtube data extracted into SQL Database
if st.button('Load Data into SQL Database'):
    query = '''select channel_name, channel_videocount from project_yt.channel
            order by channel_videocount desc'''
    mycursor.execute('select channel_id from project_yt.channel where channel_id=%s',[channel_id])
    mydb.commit()
    out=mycursor.fetchall()    

    if out:
        st.success('Channel Details of the given channel id already available in Database')
    else:
        channel_df.to_sql('channel',con=engine, if_exists='append', index=False)
        video_df.to_sql('video',con=engine, if_exists='append', index=False)
        comment_df.to_sql('comment',con=engine, if_exists='append', index=False)
        mydb.commit()
        st.success('All data extracted for channel, video, comment are loaded into SQL Database Successfully')


# creating selection button with given sql questions 
questions=st.selectbox(":orange[SELECT QUESTION]",("select option",
        "1 . What are the names of all the videos and their corresponding channels?",
        "2 . Which channels have the most number of videos, and how many videos dothey have?",
        "3 . What are the top 10 most viewed videos and their respective channels?",
        "4 . How many comments were made on each video, and what are their corresponding video names?",
        "5 . Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6 . What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "7 . What is the total number of views for each channel, and what are their corresponding channel names?",
        "8 . What are the names of all the channels that have published videos in the year 2022?",
        "9 . What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10 . Which videos have the highest number of comments, and what are their corresponding channel names?"))

if questions=="1 . What are the names of all the videos and their corresponding channels?":
    
    mycursor.execute('select channel_name,vedio_title from project.vedio')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['channel_Name','vedio_Title']))

elif questions=="2 . Which channels have the most number of videos, and how many videos dothey have?": 
    
    mycursor.execute('''select channel_name,channel_vedioCount from project.channel 
                    order by channel_vedioCount desc''')
    data=mycursor.fetchall() 
    st.write(pd.DataFrame(data,columns=['Channel_Name','Vedio_Count']))

elif questions=="3 . What are the top 10 most viewed videos and their respective channels?": 
    
    mycursor.execute('''select channel_name,vedio_title,vedio_view from project.vedio
                 order by vedio_view desc 
                 limit 10 ''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['CHANNEL','VEDIO','VIEWS'],index=[1,2,3,4,5,6,7,8,9,10]))

elif questions=="4 . How many comments were made on each video, and what are their corresponding video names?": 
   
    mycursor.execute('''select vedio_title,vedio_comment,channel_name from project.vedio''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['VEDIO','COMMENT_COUNT','CHANNEL']))

elif questions=="5 . Which videos have the highest number of likes, and what are their corresponding channel names?": 
    
    mycursor.execute('''select channel_name,vedio_title,vedio_likes from project.vedio
                 order by vedio_likes desc''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['CHANNEL','VEDIO_TITLE','LIKES_COUNT']))

elif questions=="6 . What is the total number of likes and dislikes for each video, and what are their corresponding video names?": 

    mycursor.execute('''select vedio_title , vedio_likes from project.vedio ''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['VEDIO_TITLE','NO OF LIKES']))

elif questions=="7 . What is the total number of views for each channel, and what are their corresponding channel names?":

    mycursor.execute('''select channel_name , channel_viewCount from project.channel''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['CHANNEL NAME','VIEW COUNT']))

elif questions=="8 . What are the names of all the channels that have published videos in the year 2022?": 
    mycursor.execute('''select channel_name,vedio_title,vedio_date from project.vedio
                 where extract(year from vedio_date)=2022 ''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['CHANNEL NAME','VEDIO TITLE','VEDIO DATE']))

elif questions=="9 . What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    mycursor.execute('''select channel_name,avg(vedio_duriation) from project.vedio
                 group by channel_name ''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['CHANNEL NAME','VEDIO DURIATION in seconds']))

elif questions== "10 . Which videos have the highest number of comments, and what are their corresponding channel names?":

    mycursor.execute('''select channel_name,vedio_title,vedio_comment from project.vedio
                 order by vedio_comment desc''')
    data=mycursor.fetchall()
    st.write(pd.DataFrame(data,columns=['CHANNEL NAME','VEDIO TITLE','COMMENT COUNT']))



# Final Step
st.success('successfully inserted data into local host database!', icon="✅")