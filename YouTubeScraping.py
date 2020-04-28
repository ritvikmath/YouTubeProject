#!/usr/bin/env python
# coding: utf-8

# In[364]:


import sqlite3
import pandas as pd
from random import sample, choice, random
from datetime import datetime
from time import sleep
import matplotlib.pyplot as plt
import sys

import os
import pickle
import google.oauth2.credentials
from email.mime.text import MIMEText
import base64

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import warnings
warnings.filterwarnings('ignore')


# # Database Setup

# In[365]:


def display_as_table(data, headers):
    df = pd.DataFrame(data=data, columns=[i[0] for i in headers])
    return df


# In[366]:


def create_tables():
    try:
    
        #create a database connection
        conn = sqlite3.connect('videos.db')

        #create a cursor
        cur = conn.cursor()
        
        cur.execute('''
        CREATE TABLE 
        Videos (UploadsID text, VideoID text UNIQUE, Title text, Description text, ThumbnailURL text, PublishedDate text, Duration integer, Definition text);
        ''')
        
        print('Created Videos Table')
        
        conn.commit()

        cur.execute('''
        CREATE TABLE 
        VideoStats (VideoID text, Timestamp text, Views integer, Likes integer, Dislikes integer, Comments integer);
        ''')
        
        print('Created VideoStats Table')
        
        conn.commit()

        cur.execute('''
        CREATE TABLE 
        Channels (ChannelID text UNIQUE, UploadsID text, Title text, Description text, PublishedDate text, NumVideos integer);
        ''')
        
        print('Created Channels Table')
        
        conn.commit()
        
    finally:
        #close connection
        conn.close()


# # API Setup

# In[367]:


def get_authenticated_service():
    credentials = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
    #  Check if the credentials are invalid or do not exist
    if not credentials or not credentials.valid:
        # Check if the credentials have expired
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_console()

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)

    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

def get_authenticated_gmail_service():
    credentials = None
    API_SERVICE_NAME = 'gmail'
    API_VERSION = 'v1'
    
    if os.path.exists('token_gmail.pickle'):
        with open('token_gmail.pickle', 'rb') as token:
            credentials = pickle.load(token)

    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

# # GMAIL FUNCTIONS
    
def create_message(sender, to, subject, message_text):
    """Create a message for an email.

    Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.

    Returns:
    An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}

def send_message(service, user_id, message):
    """Send an email message.

    Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.

    Returns:
    Sent Message.
    """
    
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
                   .execute())
        print('Message Id: %s' % message['id'])
        return message
    except Exception as e:
        print('An error occurred: %s' % e)


# # Core Functions

# In[368]:


def try_get_metric(result, search_list):
    """
    Tries to get a metric from a dictionary returned by YouTube API
    """
    for search in search_list:
        try:
            result = result[search]
        except:
            return None
    return result


# In[369]:


def populate_channel_data(service, cur, conn):
    """
    Populate data for all channels which do not yet have it
    """
    
    #get list of all channels which don't have all their data filled in
    cur.execute('SELECT ChannelId FROM Channels WHERE Title IS NULL')
    channel_ids = [item[0] for item in cur.fetchall()]
    
    #call API to get info on the channels
    results = service.channels().list(part='snippet, contentDetails', id=','.join(channel_ids)).execute()
    
    #for each returned channel info...
    for result in results['items']:
        
        #get the key pieces of info
        channelId = try_get_metric(result, ['id'])
        title = try_get_metric(result, ['snippet', 'title'])
        desc = try_get_metric(result, ['snippet', 'description'])
        publishedAt = try_get_metric(result, ['snippet', 'publishedAt'])
        uploadsId = try_get_metric(result, ['contentDetails', 'relatedPlaylists', 'uploads'])
        
        #update the Channels table accordingly
        statement = "UPDATE Channels SET Title=?, UploadsID=?, Description=?, PublishedDate=? WHERE ChannelId = ?"
        cur.executemany(statement, [(title, uploadsId, desc, publishedAt, channelId)])
        
        conn.commit()
    
    #return number of channels updated
    return channel_ids


# In[370]:


def add_channels_to_track(channel_ids):
    channel_ids = [(channel_id,) for channel_id in channel_ids]
    try:
        #create a database connection
        conn = sqlite3.connect('videos.db')

        #create a cursor
        cur = conn.cursor()
        
        #enter these rows into the table
        cur.executemany('INSERT INTO Channels (ChannelID) VALUES (?)', channel_ids)
        
        conn.commit()
        
    finally:
        #close connection
        conn.close()


# In[371]:


def get_channels_to_analyze(service, cur, conn):
    """
    Given a list of channel ids we care about, return a subset which has new videos
    """
    
    #get info from each channel
    cur.execute('SELECT ChannelID, UploadsID, NumVideos FROM Channels')
    result = cur.fetchall()
    channel_ids = [item[0] for item in result]
    upload_ids = [item[1] for item in result]
    num_vids = [item[2] for item in result]
    
    #create a helper dictionary
    channel_info = {}
    for i in range(len(channel_ids)):
        channel_info[channel_ids[i]] = {'upload_id': upload_ids[i], 'last_num_vids': num_vids[i]}
    
    #get statistics on each channel
    batch_size = 50
    results = []
    num_batches = int(len(channel_ids) / batch_size) + 1
    for i in range(num_batches):
        curr_channel_ids = channel_ids[batch_size*i : batch_size*(i+1)]
        curr_results = service.channels().list(part='statistics', id=','.join(curr_channel_ids)).execute()
        results.extend(curr_results['items'])
    
    #this will be the final list to return
    uploads_to_analyze = []
    
    #for each resulting channel stats...
    for result in results:
        #get channel id and current number of videos
        channel_id = try_get_metric(result, ['id'])
        num_videos = int(try_get_metric(result, ['statistics', 'videoCount']))
        
        #get last recorded number of videos
        last_num_videos = channel_info[channel_id]['last_num_vids']
        
        #if there is discrepency, new videos have been added
        if last_num_videos != num_videos:
            
            #add this uploads playlist for analysis
            uploads_to_analyze.append(channel_info[channel_id]['upload_id'])
            
            #update the table with latest number of videos recorded
            cur.execute('UPDATE Channels SET NumVideos = %s WHERE ChannelId = "%s"'%(num_videos, channel_id))
            conn.commit()
    
    #if this is a four hour timepoint, do a full data collection for safety
    if datetime.now().hour % 4 == 2:
        return upload_ids, []
    
    #return the uploads we need to reanalyze and all the others
    return uploads_to_analyze, [upload for upload in upload_ids if upload not in uploads_to_analyze]


# In[372]:


def get_most_recent_videos(service, upload_ids, num_vids):
    """
    Given channel ids and number of most recent videos, get information about those videos in a dictionary
    """
    
    #this will be the final dictionary to return
    dict_of_results = {}
    
    #for each upload playlist we wish to analyze...
    for upload_id in upload_ids:
        
        #call the api to get the most recent videos
        results = service.playlistItems().list(part='snippet', playlistId=upload_id, maxResults=num_vids).execute()
        #for each returned video in this upload playlist...
        for result in results['items']:
            
            #get relevant fields
            vid_id = try_get_metric(result, ['snippet', 'resourceId', 'videoId'])
            published_date = try_get_metric(result, ['snippet', 'publishedAt'])
            thumbnail_url = try_get_metric(result, ['snippet', 'thumbnails', 'high', 'url'])
            title = try_get_metric(result, ['snippet', 'title'])
            description = try_get_metric(result, ['snippet', 'description'])

            #put those fields in the dictionary with key as this video id
            dict_of_results[vid_id] = {'upload_id': upload_id, 'title':title, 'description':description, 'thumbnail_url':thumbnail_url, 'published_date':published_date}
    
    #return all information
    return dict_of_results


# In[373]:


def get_old_video_ids(cur, other_upload_ids):
    #reformat the input list to be a comma separated string
    other_upload_ids = ['"%s"'%upload_id for upload_id in other_upload_ids]
    other_upload_ids = ','.join(other_upload_ids)
    
    #get most recent K videos for each of the upload playlists we are not reanalyzing
    cur.execute('''
    SELECT VideoId
    FROM (
        SELECT VideoId, Rank() 
          over (PARTITION BY UploadsId
                ORDER BY PublishedDate DESC ) AS Rank
        FROM Videos
        WHERE UploadsId IN (%s)
        ) WHERE Rank <= 10
    '''%(other_upload_ids))
    
    old_vid_ids = [item[0] for item in cur.fetchall()]
    
    #return all the old videos
    return old_vid_ids


# In[374]:


def get_most_recent_video_stats(service, vid_ids):
    """
    Given a string of video ids, get current statistics on those videos
    """
    
    #this will be the return dictionary of results
    dict_of_results = {}
    
    #we can only put 50 vids in at once so calculate number of batches we would need
    num_batches = int(len(vid_ids) / 50) + 1
    
    #get a common timestamp for all videos written in this time period
    timestamp = datetime.now()
    
    for i in range(num_batches):
        curr_vid_ids = vid_ids[50*i:50*(i+1)]
        curr_vid_ids = ','.join(curr_vid_ids)
        
        #call API to get data on these videos
        results = service.videos().list(part='contentDetails, statistics', id=curr_vid_ids).execute()

        #for each video statistic returned...
        for result in results['items']:
            
            #get key relevant info
            vid_id = try_get_metric(result, ['id'])
            curr_views = try_get_metric(result, ['statistics', 'viewCount'])
            curr_likes = try_get_metric(result, ['statistics', 'likeCount'])
            curr_dislikes = try_get_metric(result, ['statistics', 'dislikeCount'])
            curr_comments = try_get_metric(result, ['statistics', 'commentCount'])
            duration = try_get_metric(result, ['contentDetails', 'duration'])
            definition = try_get_metric(result, ['contentDetails', 'definition'])
            
            #add this data to the dictionary
            dict_of_results[vid_id] = {'timestamp': timestamp, 'views': curr_views, 'likes': curr_likes, 'dislikes': curr_dislikes, 'comments': curr_comments, 'duration': duration, 'definition': definition}
    
    #return the dictionary of info
    return dict_of_results


# In[375]:


def insert_into_videos(cur, recent_videos):
    """
    Given dictionary of recent videos, insert them into the Videos table if they are not already there
    """
    
    #get the ids of the videos to insert
    video_ids = list(recent_videos.keys())
    
    #get the existing video ids from the table
    cur.execute('SELECT VideoID FROM Videos')
    existing_video_ids = [item[0] for item in cur.fetchall()]
    
    #get the video ids we need to insert
    video_ids_to_insert = [vid_id for vid_id in video_ids if vid_id not in existing_video_ids]
    
    #gather a list of rows to insert
    rows_to_insert = [(data['upload_id'], vid_id, data['title'], data['description'], data['thumbnail_url'], data['published_date'], None, None) for vid_id, data in recent_videos.items() if vid_id in video_ids_to_insert]
    
    #enter these rows into the table
    cur.executemany('INSERT INTO Videos VALUES (?,?,?,?,?,?,?,?)', rows_to_insert)
    
    #return the number of new videos inserted
    return video_ids_to_insert


# In[376]:


def insert_into_video_statistics(cur, video_stats):
    """
    Given dictionary of video statistics, insert them into the VideoStats table.
    Also, if this dictionary carries any info about duration or definition, update that in the Videos table
    """
    
    #get the list of rows to insert
    rows_to_insert = [(vid_id, data['timestamp'], data['views'],                        data['likes'], data['dislikes'], data['comments'])                       for vid_id, data in video_stats.items()]
    
    #enter these rows into the table
    cur.executemany('INSERT INTO VideoStats VALUES (?,?,?,?,?,?)', rows_to_insert)
    
    #additionally, we only got the duration and definition here so figure
    #out which videos still don't have values for these two fields
    cur.execute('SELECT VideoId FROM Videos WHERE (Duration IS NULL OR Definition IS NULL)')
    video_ids_with_null_data = [item[0] for item in cur.fetchall() if item[0] in video_stats.keys()]
    
    #for each video with missing data...
    for vid_id in video_ids_with_null_data:
        #fill in the data
        statement = "UPDATE Videos SET Duration=?, Definition=? WHERE VideoID=?"
        cur.executemany(statement, [(video_stats[vid_id]['duration'], video_stats[vid_id]['definition'], vid_id)])
    
    #return the number of stats inserted and number of videos updated
    return len(rows_to_insert), len(video_ids_with_null_data)


# In[377]:


def delete_all_data(full=False):
    """
    This method is for testing and lets us delete or drop all tables to start fresh
    """
    try:
        if os.path.exists("youtube_status_logging.txt"):
            os.remove("youtube_status_logging.txt")
        if os.path.exists("youtube_error_logging.txt"):
            os.remove("youtube_error_logging.txt")

        #create a database connection
        conn = sqlite3.connect('videos.db')

        #create a cursor
        cur = conn.cursor()

        print("THE FOLLOWING OPERATION WILL EMPTY OR DROP ALL TABLES!")
        print("TYPE THE FOLLOWING CODE TO CONFIRM")
        h = str(hash(random()))[:4]
        print(h)
        user_value = input()
        if user_value == h:
            if full:
                cur.execute('DROP TABLE Videos;')
                cur.execute('DROP TABLE VideoStats;')
                cur.execute('DROP TABLE Channels;')
                print('DROPPED TABLES')
            else:
                cur.execute('DELETE FROM Videos;')
                cur.execute('DELETE FROM VideoStats;')
                cur.execute('UPDATE Channels SET NumVideos=NULL, Title=NULL, Description=NULL, PublishedDate=NULL;')
                print('DELETED TABLE DATA')
            conn.commit()
            
        else:
            print('CONFIRMATION FAILED')
    
    finally:
        #close connection
        conn.commit()
        conn.close()

if __name__ == '__main__':
    
    max_retries = 3
    curr_attempt = 0
    operation_success = False
    
    #while we have not succeeded and haven't exceeded max retries
    while (not operation_success) and (curr_attempt < max_retries):
        
        curr_attempt += 1
    
        try:
            
            status_logs = open("youtube_status_logging.txt", "a+")
            error_logs = open("youtube_error_logging.txt", "a+")
                
            status_logs.write(str(datetime.now()) + '\n\n')
            
            if datetime.now().hour % 4 == 2:
                status_logs.write('Full Data Collection\n\n')
            
            #API setup
            CLIENT_SECRETS_FILE = "client_secret.json"
            SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
            API_SERVICE_NAME = 'youtube'
            API_VERSION = 'v3'
    
            os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
            service = get_authenticated_service()
            
            gmail_service = get_authenticated_gmail_service()
    
            #create a database connection
            conn = sqlite3.connect('videos.db')
    
            #create a cursor
            cur = conn.cursor()
    
            #populate data for new channels
            channels_updated = populate_channel_data(service, cur, conn)
            updated_channels_message = 'Updated Channels: %s'
            print(updated_channels_message%len(channels_updated))
            status_logs.write(updated_channels_message%channels_updated + '\n\n')
            
    
            #get channels that need analysis
            uploads_to_analyze, other_uploads = get_channels_to_analyze(service, cur, conn)
    
            #get recent videos
            recent_vids = get_most_recent_videos(service, uploads_to_analyze, 25)
    
            #insert those videos into the table    
            videos_added = insert_into_videos(cur, recent_vids)
    
            #get ids of videos for channels not just analyzed
            old_video_ids = get_old_video_ids(cur, other_uploads)
    
            #get stats for recent videos
            all_vids = list(recent_vids.keys()) + old_video_ids
    
            video_stats = get_most_recent_video_stats(service, all_vids)
            stats_rows_inserted, videos_updated = insert_into_video_statistics(cur, video_stats)
            videos_added_message = "Videos Added: %s"
            stats_inserted_message = "Stats Inserted: %s"%stats_rows_inserted
            
            print(videos_added_message%len(videos_added))
            print(stats_inserted_message)
            
            status_logs.write(videos_added_message%videos_added + '\n\n')
            status_logs.write(stats_inserted_message + '\n\n')
            status_logs.write('-------------\n\n')
            
            my_email = 'ritvikmathematics@gmail.com'
            subject = 'Logged Data on %s'%str(datetime.now())
            body = updated_channels_message%channels_updated + '\n\n' + videos_added_message%videos_added + '\n\n' + stats_inserted_message + '\n\n'
            
            if datetime.now().hour % 4 == 2:
                body = 'Full Data Collection\n\n' + body
                
            success_email = create_message(my_email, my_email, subject, body)
            send_message(gmail_service, 'me', success_email)
            
            conn.commit()
            
            operation_success = True
            
        except Exception as e:
            error_logs.write(str(datetime.now()) + '\n\n' + str(e) + '\n\nAttempt: ' + str(curr_attempt) + '\n-----------------\n\n')
            my_email = 'ritvikmathematics@gmail.com'
            subject = 'Data Collection Error on %s : Attempt %s'%(str(datetime.now()), curr_attempt)
            error_email = create_message(my_email, my_email, subject, str(e))
            send_message(gmail_service, 'me', error_email)
            
        finally:
            conn.close()
            status_logs.close()
            error_logs.close()
        
        #if the operation did not succeed, sleep for a minute before retrying
        if operation_success == False:
            sleep(60)