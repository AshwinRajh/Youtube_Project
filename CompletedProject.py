import streamlit as st
import pymongo
from googleapiclient.discovery import build
api_service_name = 'youtube'
api_version = 'v3'
api_key = "AIzaSyAm62tNRauhAqQBAXmnhOjb5GB3QLiezMs"
youtube = build(api_service_name,api_version, developerKey = api_key )
from datetime import timedelta
from googleapiclient.errors import HttpError
import pandas as pd
import mysql.connector



def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(
            part = 'snippet,contentDetails,statistics',
            id = channel_id)
    response = request.execute()
    channel = response['items'][0]
    channel_data = dict(Channel_ID = channel_id,
                Channel_name = channel['snippet']['title'],
               Subscribers = channel['statistics']['subscriberCount'],
                Views = channel['statistics']['viewCount'],
                Total_videos = channel['statistics']['videoCount'],
                Playlistv_ID = channel['contentDetails']['relatedPlaylists']['uploads'],
                       )
    return channel_data


# In[11]:


def get_playlist_id(youtube,channel_id):
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = request.execute()
    playlist = response['items'][0]

    playlist_id = playlist['contentDetails']['relatedPlaylists']['uploads']

    return playlist_id


# # Function to get PlaylistID & VideoID

# In[12]:


def get_video_ids(youtube,channel_id):
    playlist_id = get_playlist_id(youtube, channel_id)  # Call the function to get the playlist ID
    
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50

    )
    response = request.execute()
    vid = response['items']
    video_ids = []
    for i in range(len(response['items'])):
        video_ids.append((vid[i]['contentDetails']['videoId']))
        
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken = next_page_token
                         )
            response = request.execute()
                
            for i in range(len(response['items'])):
                video_ids.append((response['items'][i]['contentDetails']['videoId']))
    
            next_page_token = response.get('nextPageToken')
    
    return video_ids


# # Function to get Video Stats

# In[13]:


def get_video_stats(youtube, video_id):
    try:    
        video_stat = []
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        vstat = response['items'][0]

        # Parse the time duration string
        duration = vstat['contentDetails']['duration']

        # Initialize variables for hours, minutes, and seconds
        hours = 0
        minutes = 0
        seconds = 0

        # Find positions of 'H', 'M', and 'S' characters
        if 'H' in duration:
            hours_position = duration.find("H")
            hours = int(duration[2:hours_position])
        if 'M' in duration:
            minutes_position = duration.find("M")
            if 'H' in duration:
                minutes = int(duration[hours_position + 1:minutes_position])
            else:
                minutes = int(duration[2:minutes_position])
        if 'S' in duration:
            seconds_position = duration.find("S")
            if 'M' in duration:
                seconds = int(duration[minutes_position + 1:seconds_position])
            elif 'H' in duration:
                seconds = int(duration[hours_position + 1:seconds_position])
            else:
                seconds = int(duration[2:seconds_position])

        # Calculate duration in seconds
        result = (hours * 3600) + (minutes * 60) + seconds
    except HttpError as e:
        if e.resp.status == 403 and 'commentsDisabled' in str(e):
            # Comments are disabled for this video
            return []
        else:
            raise e



        # Create a dictionary containing video data
    video_data = {
            "video_name": vstat['snippet']['title'],
            "Likes": vstat['statistics']['likeCount'],
            "Views": vstat['statistics']['viewCount'],
            "Total_Count":vstat['statistics'].get('commentCount', 0),
            "Duration": result,  # Duration in seconds
            "Video_id": video_id
            }
    return video_data


# # Function to get comments

# In[14]:


def get_video_comments(youtube,video_id):
    try:
        all_video_comments = []  # Initialize a list to store comments for all videos

        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            maxResults=50
        )
        response = request.execute()

        video_comments = []  # Initialize a list to store comments for the current video
            
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            video_comments.append(comment)  # Append comment to the list of comments for this video
            
        next_page_token = response.get('nextPageToken')
        more_pages = True
        
        while more_pages:
            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.commentThreads().list(
                            part='snippet,replies',
                            videoId=video_id,
                            maxResults=50,
                            pageToken = next_page_token    
                            )
                response = request.execute()

                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    video_comments.append(comment)
                next_page_token = response.get('nextPageToken')
                
                
            # Append the list of comments for this video to the overall list
        return video_comments
    except HttpError as e:
        if e.resp.status == 403 and 'commentsDisabled' in str(e):
            # Comments are disabled for this video
            return []
        else:
            raise e


# # Combine all Youtube Data

# In[15]:



def main(youtube,channel_id):
    channel_stats = get_channel_stats( youtube, channel_id)
    video_ids = get_video_ids (youtube, channel_id)
    playlist_name = get_playlist_id(youtube,channel_id)
    video_stats = [get_video_stats( youtube,video_id) for video_id in video_ids]
    video_comments = [get_video_comments( youtube,video_id) for video_id in video_ids]


    youtube_data ={
        "channel details": channel_stats,
        "playlist ID" : playlist_name,
        "video details":video_stats,
        "comment details":video_comments,
        }
    return(youtube_data)




# ========================================   /     Data collection zone and Stored data to MongoDB    /   =================================== #
 # Replace with the desired channel ID
#channel_info = main(youtube, channel_id)
#print(channel_info)


# Replace <your_connection_string> with your actual MongoDB Atlas connection string
connection_string = "mongodb+srv://AshwinRajh:Spartans%401234@cluster0.a9kksg9.mongodb.net/"

# Establish a connection to the MongoDB Atlas cluster
client = pymongo.MongoClient(connection_string)

# Select a database
db = client['Data_Youtube']

# Select a collection to store user data
collection = db['Reqd_Details']


st.set_page_config(layout='wide')

# Title
st.title(':red[Youtube Data Harvesting]')

# Data collection zone
col1, col2 = st.columns(2)
with col1:
    st.header(':violet[Data collection zone]')
    st.write('(Note:- This zone **collects data** by using a channel ID and **stores it in the :green[MongoDB] database**.)')
    channel_id = st.text_input('**Enter 11-digit channel_id**')
    Get_data = st.button('**Get data and store**')

if Get_data:
    total_data = main(youtube, channel_id)
    result = collection.insert_one(total_data)

    if result.acknowledged:
        st.write(f"Data inserted with document ID: {result.inserted_id}")
    else:
        st.write("Insertion failed.")


# Close MongoDB client
client.close()


#sql  begins.....



# ========================================   /     Data Migrate zone (Stored data to MySQL)    /   ========================================== #

with col2:
    st.header(':violet[Data Migrate zone]')
    st.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')
    SQL_Button = st.button("Add data to SQL")



connect = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "9524320874",
        auth_plugin = "mysql_native_password")


mycursor = connect.cursor()

# Create the database if it doesn't exist
mycursor.execute("CREATE DATABASE IF NOT EXISTS YoutubeSQL")

# Switch to the YoutubeSQL database
mycursor.execute("USE YoutubeSQL")

# Create the Channel table
mycursor.execute("""
    CREATE TABLE IF NOT EXISTS Channel (
        id VARCHAR(255) NOT NULL, 
        chname VARCHAR(255) NOT NULL,
        subra INT,
        views INT,
        total INT,
        playlist VARCHAR(255),
        PRIMARY KEY (id)
    )
""")

mycursor.execute("""
    CREATE TABLE IF NOT EXISTS Playlist (
        Playlistid VARCHAR(255),
        videoid VARCHAR(255),
        chid VARCHAR(255) NOT NULL,
        FOREIGN KEY (chid) REFERENCES Channel(id),
        INDEX videoid_index (videoid)
    );
""")
    
                 
mycursor.execute("""CREATE TABLE IF NOT EXISTS Video_Details (
    vid VARCHAR(255) NOT NULL,
    videoname VARCHAR(255) NOT NULL,
    likes INT,
    views INT,
    total_count INT,
    duration INT,
    chname VARCHAR(255),
    FOREIGN KEY (vid) REFERENCES Playlist(videoid),
    INDEX videoid_index (vid)

);
""")
                 

mycursor.execute("""CREATE TABLE IF NOT EXISTS Comments (
    comment_id INT AUTO_INCREMENT PRIMARY KEY,
    comment TEXT,
    video_id VARCHAR(255) NOT NULL,
    FOREIGN KEY (video_id) REFERENCES Playlist(videoid),
    INDEX videoid_index (video_id)
);
    """)



# import mysql.connector

# # Establish a connection to MySQL
# connect = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="9524320874",
#     database="YoutubeSQL"  # Specify the databasUCbZr6zxHZq0eFpVBeU-7IgQe to use
# )

# Create a cursor
mycursor = connect.cursor()

if SQL_Button:
# Get channel data
    channel_data = get_channel_stats(youtube, channel_id)

    # SQL INSERT statement for the Channel table
    insert_channel_query = "INSERT INTO Channel (chname, subra, views, total, playlist,id) VALUES (%s, %s, %s, %s, %s,%s)"

    # Execute the INSERT statement for the Channel table
    mycursor.execute(insert_channel_query, (
        channel_data['Channel_name'],
        channel_data['Subscribers'],
        channel_data['Views'],
        channel_data['Total_videos'],
        channel_data['Playlistv_ID'],
        channel_id
    ))

    # Commit the changes to the database
    connect.commit()

    # Get playlist data
    playlist_name = get_playlist_id(youtube, channel_id)
    video_ids = get_video_ids(youtube, channel_id)

    # SQL INSERT statement for the Playlist table
    # Execute the INSERT statement for the Playlist table
    for item in  video_ids:
        print(str(item))
        mycursor.execute(f"INSERT INTO Playlist (Playlistid, videoid, chid) VALUES ('{playlist_name}','{str(item)}','{channel_id}')")

    # Commit the changes to the database
    connect.commit()

    video_data_list= [get_video_stats( youtube,video_id) for video_id in video_ids]


    # SQL INSERT statement for the Video_Details table
    insert_video_query = "INSERT INTO Video_Details (videoname, likes, views, total_count, duration, vid) VALUES (%s, %s, %s, %s, %s,%s)"

    # Execute the INSERT statement for the Video_Details table for each video
    for video_data in video_data_list:
        mycursor.execute(insert_video_query, (
            video_data['video_name'],
            video_data['Likes'],
            video_data['Views'],
            video_data['Total_Count'],
            video_data['Duration'],
            video_data['Video_id'],


        ))

    connect.commit()


    comment_data_list= [get_video_comments( youtube,video_id) for video_id in video_ids]

    # SQL IUCbZr6zxHZq0eFpVBeU-7IgQNSERT statement for the Comments table
    insert_comment_query = "INSERT INTO Comments (comment,video_id) VALUES (%s,%s)"

    # Execute the INSERT statement for the Comments table for each comment
    for comment_data in zip(video_ids, comment_data_list):
        for comment in comment_data[1]:
            mycursor.execute(insert_comment_query, (
            comment,
            comment_data[0]
        ))

    # Commit the changes to the database
    connect.commit()

    st.success("Document uploaded to MySQL!")



#analyse in sql
with col2:
# Define the list of available analysis options
    options = [
        "What are the Names of all the videos and their corresponding channels?",
        "Which Top 5 channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?"
    ]

    # Streamlit app title and description
    st.title('Data Analysis Options')
    st.write('Choose a data analysis option from the list below:')

    # Create a selectbox to choose the analysis option
    selected_option = st.selectbox("Choose Data Analysis Option ⬇️", options)

# Function to execute the selected analysis option
def execute_analysis(selected_option):
    # Add code to execute SQL queries based on the selected_option
    if selected_option == "What are the Names of all the videos and their corresponding channels?":
        st.write("Executing Query 1...")
        query_1 = """
            SELECT
                ch.chname AS Channel_Name,
                vd.videoname AS Video_Title
            FROM
                Channel AS ch
            JOIN
                Playlist AS pl ON ch.id = pl.chid
            JOIN
                Video_Details AS vd ON pl.videoid = vd.vid;
            """
        mycursor.execute(query_1)
        data_1 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_1, columns=["Channel Names", "Video Title"], index=range(1, len(data_1) + 1)))
        st.success("DONE", icon="✅")

    elif selected_option == "Which Top 5 channels have the most number of videos, and how many videos do they have?":
        st.write("Executing Query 2...")
        query_2 = """
            SELECT
                CH.chname AS Channel_Name,
                COUNT(PL.videoid) AS Number_of_Videos
            FROM
                Channel AS CH
            JOIN
                Playlist AS PL ON CH.id = PL.chid
            GROUP BY
                CH.chname
            ORDER BY
                Number_of_Videos DESC
            LIMIT 5;
            """
        mycursor.execute(query_2)
        data_2 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_2, columns=["Channel Names", "Number of Videos"], index=range(1, len(data_2) + 1)))
        st.success("DONE", icon="✅")

    elif selected_option == "What are the top 10 most viewed videos and their respective channels?":
        st.write("Executing Query 3...")
        query_3 = """
            SELECT
                CH.chname AS Channel_Name,
                VD.videoname AS Video_Title,
                VD.Views AS Views
            FROM
                Video_Details AS VD
            JOIN
                Playlist AS PL ON VD.vid = PL.videoid
            JOIN
                Channel AS CH ON PL.chid = CH.id
            ORDER BY
                Views DESC
            LIMIT 10;
            """
        mycursor.execute(query_3)
        data_3 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_3, columns=["Channel Names", "Video Title", "Views"], index=range(1, len(data_3) + 1)))
        st.success("DONE", icon="✅")

    elif selected_option == "How many comments were made on each video, and what are their corresponding video names?":
        st.write("Executing Query 4...")
        query_4 = """
            SELECT
                VD.videoname AS Video_Title,
                COUNT(CM.comment_id) AS Number_of_Comments
            FROM
                Video_Details AS VD
            JOIN
                Playlist AS PL ON VD.vid = PL.videoid
            LEFT JOIN
                Comments AS CM ON PL.videoid = CM.video_id
            GROUP BY
                VD.videoname;
            """
        mycursor.execute(query_4)
        data_4 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_4, columns=["Video Title", "Number of Comments"], index=range(1, len(data_4) + 1)))
        st.success("DONE", icon="✅")

    elif selected_option == "Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?":
        st.write("Executing Query 5...")
        query_5 = """
            SELECT
                CH.chname AS Channel_Name,
                VD.videoname AS Video_Title,
                VD.Likes AS Likes
            FROM
                Video_Details AS VD
            JOIN
                Playlist AS PL ON VD.vid = PL.videoid
            JOIN
                Channel AS CH ON PL.chid = CH.id
            ORDER BY
                Likes DESC
            LIMIT 10;
            """
        mycursor.execute(query_5)
        data_5 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_5, columns=["Channel Name", "Video Title", "Likes"], index=range(1, len(data_5) + 1)))
        st.success("DONE", icon="✅")


    elif selected_option == "What is the total number of views for each channel, and what are their corresponding channel names?":
        st.write("Executing Query 6...")
        query_6 = """
            SELECT
                CH.chname AS Channel_Name,
                SUM(VD.Views) AS Total_Views
            FROM
                Video_Details AS VD
            JOIN
                Playlist AS PL ON VD.vid = PL.videoid
            JOIN
                Channel AS CH ON PL.chid = CH.id
            GROUP BY
                CH.chname;
            """
        mycursor.execute(query_6)
        data_6 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_6, columns=["Channel Name", "Total Views"], index=range(1, len(data_6) + 1)))
        st.success("DONE", icon="✅")



    elif selected_option == "What are the names of all the channels that have published videos in the year 2022?":
        st.write("Executing Query 7...")
        query_7 = """
            SELECT
                DISTINCT CH.chname AS Channel_Name
            FROM
                Channel AS CH
            JOIN
                Playlist AS PL ON CH.id = PL.chid
            JOIN
                Video_Details AS VD ON PL.videoid = VD.vid
            WHERE
                YEAR(VD.Publish_Date) = 2022;
            """
        mycursor.execute(query_7)
        data_7 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_7, columns=["Channel Name"], index=range(1, len(data_7) + 1)))
        st.success("DONE", icon="✅")


    elif selected_option == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        st.write("Executing Query 8...")
        query_8 = """
            SELECT
                CH.chname AS Channel_Name,
                AVG(VD.Duration) AS Average_Duration
            FROM
                Video_Details AS VD
            JOIN
                Playlist AS PL ON VD.vid = PL.videoid
            JOIN
                Channel AS CH ON PL.chid = CH.id
            GROUP BY
                CH.chname;
            """
        mycursor.execute(query_8)
        data_8 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_8, columns=["Channel Name", "Average Duration"], index=range(1, len(data_8) + 1)))
        st.success("DONE", icon="✅")
    


    elif selected_option == "Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?":
        st.write("Executing Query 9...")
        query_9 = """
            SELECT
                CH.chname AS Channel_Name,
                VD.videoname AS Video_Title,
                COUNT(CM.comment_id) AS Number_of_Comments
            FROM
                Video_Details AS VD
            JOINc
                Playlist AS PL ON VD.vid = PL.videoid
            JOIN
                Channel AS CH ON PL.chid = CH.id
            LEFT JOIN
                Comments AS CM ON PL.videoid = CM.video_id
            GROUP BY
                CH.chname, VD.videoname
            ORDER BY
                Number_of_Comments DESC
            LIMIT 100;
            """
        mycursor.execute(query_9)
        data_9 = [i for i in mycursor.fetchall()]
        st.dataframe(pd.DataFrame(data_9, columns=["Channel Name", "Video Title", "Number of Comments"], index=range(1, len(data_9) + 1)))
        st.success("DONE", icon="✅")


with col2:
    if st.button("Execute Analysis"):
        execute_analysis(selected_option)


