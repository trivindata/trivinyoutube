# Import required libraries
import mysql.connector
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import datetime

# function to create a connection to the MySQL database
def create_connection():
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Raji@123",
        database="youtube"
    )
    return connection

# function to fetch data from the MySQL database
def fetch_data(query):
    connection = create_connection()
    df = pd.read_sql(query, connection)
    connection.close()
    return df

# function to create tables
def create_tables():
    connection = create_connection()
    cursor = connection.cursor()

    #queries to create tables
    create_channel_table_query = '''
    CREATE TABLE IF NOT EXISTS channel_details (
        Channel_Id VARCHAR(80) PRIMARY KEY,
        Channel_Name VARCHAR(100),
        Subscribers BIGINT,
        Views BIGINT,
        Total_Videos INT,
        Channel_Description TEXT,
        Playlist_Id VARCHAR(80)
    )
    '''

    create_playlist_table_query = '''
    CREATE TABLE IF NOT EXISTS playlist_information (
        Playlist_Id VARCHAR(100) PRIMARY KEY,
        Title VARCHAR(100),
        Channel_Id VARCHAR(100),
        Channel_Name VARCHAR(100),
        PublishedAt TIMESTAMP,
        Video_Count INT
    )
    '''

    create_video_table_query = '''
    CREATE TABLE IF NOT EXISTS video_information (
        Video_Id VARCHAR(30) PRIMARY KEY,
        Channel_Name VARCHAR(100),
        Channel_Id VARCHAR(100),
        Title VARCHAR(150),
        Tags TEXT,
        Thumbnail VARCHAR(200),
        Description TEXT,
        Published_Date TIMESTAMP,
        Duration VARCHAR(50),
        Views BIGINT,
        Likes BIGINT,
        Comments INT,
        Favorite_Count INT,
        Definition VARCHAR(10),
        Caption_Status VARCHAR(50)
    )
    '''

    create_comment_table_query = '''
    CREATE TABLE IF NOT EXISTS comment_information (
        Comment_Id VARCHAR(100) PRIMARY KEY,
        Video_Id VARCHAR(50),
        Comment_Text TEXT,
        Comment_Author VARCHAR(150),
        Comment_Published TIMESTAMP
    )
    '''

    # Execution queries
    cursor.execute(create_channel_table_query)
    cursor.execute(create_playlist_table_query)
    cursor.execute(create_video_table_query)
    cursor.execute(create_comment_table_query)

    # Commit changes and close connection
    connection.commit()
    connection.close()

# function to collect data from the YouTube API and store it in the database
def collect_and_store_data(channel_id):
    # Establish a connection to the YouTube API
    youtube = build("youtube", "v3", developerKey="AIzaSyDI9UcnptDL8DpCehGL1NZJa_3j8jK9Wlw")

    # Retrieve channel information
    channel_info_request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    channel_info_response = channel_info_request.execute()

    channel_data = channel_info_response["items"][0]
    channel_snippet = channel_data["snippet"]
    channel_statistics = channel_data["statistics"]
    channel_content_details = channel_data["contentDetails"]

    # Extract relevant data from the response
    channel_name = channel_snippet["title"]
    subscribers = int(channel_statistics["subscriberCount"])
    views = int(channel_statistics["viewCount"])
    total_videos = int(channel_statistics["videoCount"])
    channel_description = channel_snippet.get("description", "")
    playlist_id = channel_content_details["relatedPlaylists"]["uploads"]

    # Store channel information in the database
    connection = create_connection()
    cursor = connection.cursor()

    insert_channel_query = '''
    INSERT INTO channel_details (Channel_Id, Channel_Name, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    channel_values = (channel_id, channel_name, subscribers, views, total_videos, channel_description, playlist_id)
    cursor.execute(insert_channel_query, channel_values)
    connection.commit()

    # Retrieve videos from the channel's playlist
    video_ids = []
    next_page_token = None

    while True:
        playlist_items_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        playlist_items_response = playlist_items_request.execute()

        for item in playlist_items_response["items"]:
            video_ids.append(item["snippet"]["resourceId"]["videoId"])

        next_page_token = playlist_items_response.get("nextPageToken")

        if not next_page_token:
            break

    # Retrieve video details and store them in the database
    for video_id in video_ids:
        video_info_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        video_info_response = video_info_request.execute()

        video_data = video_info_response["items"][0]
        video_snippet = video_data["snippet"]
        video_content_details = video_data["contentDetails"]
        video_statistics = video_data["statistics"]

        # Extract relevant data from the response
        title = video_snippet["title"]
        tags = video_snippet.get("tags", [])
        thumbnail = video_snippet["thumbnails"]["default"]["url"]
        description = video_snippet.get("description", "")
        published_at = datetime.datetime.strptime(video_snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
        duration = video_content_details["duration"]
        views = int(video_statistics.get("viewCount", 0))
        likes = int(video_statistics.get("likeCount", 0))
        comments = int(video_statistics.get("commentCount", 0))
        favorite_count = int(video_statistics.get("favoriteCount", 0))
        definition = video_content_details["definition"]
        caption_status = video_content_details["caption"]

        # Store video information in the database
        insert_video_query = '''
        INSERT INTO video_information (Video_Id, Channel_Name, Channel_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        video_values = (video_id, channel_name, channel_id, title, ",".join(tags), thumbnail, description, published_at, duration, views, likes, comments, favorite_count, definition, caption_status)
        cursor.execute(insert_video_query, video_values)

    # Commit changes and close connection
    connection.commit()
    connection.close()

    return "Data collected and stored successfully."

# Streamlit interface
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

channel_id = st.text_input("Enter the channel ID")

if st.button("Collect and store data"):
    if channel_id:
        create_tables()  # Create tables if not exists
        collect_and_store_data(channel_id)
        st.success("Data collected and stored successfully.")
    else:
        st.error("Please enter a valid channel ID.")


st.write("## Data Analysis")
st.write("Choose a question from the dropdown menu to analyze the data:")

#questions and corresponding SQL queries
questions = [
    "All the videos and the channel name",
    "Channels with the most number of videos",
    "10 most viewed videos",
    "Comments in each video",
    "Videos with the highest likes",
    "Likes of all videos",
    "Views of each channel",
    "Videos published in the year 2022",
    "Average duration of all videos in each channel",
    "Videos with the highest number of comments"
]

queries = [
    '''SELECT Title AS videos, Channel_Name AS channelname FROM video_information''',
    '''SELECT Channel_Name AS channelname, Total_Videos AS no_videos FROM channel_details ORDER BY Total_Videos DESC''',
    '''SELECT Views AS views, Channel_Name AS channelname, Title AS videotitle FROM video_information WHERE Views IS NOT NULL ORDER BY Views DESC LIMIT 10''',
    '''SELECT Comments AS no_comments, Title AS videotitle FROM video_information WHERE Comments IS NOT NULL''',
    '''SELECT Title AS videotitle, Channel_Name AS channelname, Likes AS likecount FROM video_information WHERE Likes IS NOT NULL ORDER BY Likes DESC''',
    '''SELECT Likes AS likecount, Title AS videotitle FROM video_information''',
    '''SELECT Channel_Name AS channelname, Views AS totalviews FROM channel_details''',
    '''SELECT Title AS video_title, Published_Date AS videorelease, Channel_Name AS channelname FROM video_information WHERE EXTRACT(YEAR FROM Published_Date) = 2022''',
    '''SELECT Channel_Name AS channelname, AVG(Duration) AS averageduration FROM video_information GROUP BY Channel_Name''',
    '''SELECT Title AS videotitle, Channel_Name AS channelname, Comments AS comments FROM video_information WHERE Comments IS NOT NULL ORDER BY Comments DESC'''
]

# UI for selecting questions
question = st.selectbox("Select your question", questions)

# Execute selected query and display results
if question:
    query_index = questions.index(question)
    query = queries[query_index]
    df = fetch_data(query)
    st.write(df)
