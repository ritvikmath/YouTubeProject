#!/usr/bin/env python
# coding: utf-8

# In[60]:


from datetime import datetime, timedelta
import sqlite3
import pandas as pd

import os
import pickle
import google.oauth2.credentials
from email.mime.text import MIMEText
import base64

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload


# # API Setup

# In[30]:


def get_authenticated_drive_service():
    credentials = None
    API_SERVICE_NAME = 'drive'
    API_VERSION = 'v3'
    
    if os.path.exists('token_drive.pickle'):
        with open('token_drive.pickle', 'rb') as token:
            credentials = pickle.load(token)

    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)


# In[31]:


def get_authenticated_gmail_service():
    credentials = None
    API_SERVICE_NAME = 'gmail'
    API_VERSION = 'v1'
    
    if os.path.exists('token_gmail.pickle'):
        with open('token_gmail.pickle', 'rb') as token:
            credentials = pickle.load(token)

    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)


# # Email Functions

# In[32]:


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


# In[33]:


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

# In[34]:


def display_as_table(data, headers):
    df = pd.DataFrame(data=data, columns=[i[0] for i in headers])
    return df


# In[35]:


def create_tables(conn, cur):
    try:

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
        
    except:
        print("Failed to create all tables")


# In[66]:


def get_most_recent_created_time(drive_service):
    """
    get the timestamp of the most recent backup added to google drive
    """
    
    query="'17S7wM_Jot5q7QK1CSvF7QADT760EBf9L' in parents"

    response = drive_service.files().list(q=query, spaces='drive', fields='files(createdTime)', orderBy="createdTime desc").execute()
    
    gmt_timestamp = response['files'][0]['createdTime'].replace('T', ' ').replace('Z', '')
    gmt_time = datetime.strptime(gmt_timestamp.split('.')[0], "%Y-%m-%d %H:%M:%S") - timedelta(hours=7)
    local_timestamp = datetime.strftime(gmt_time, "%Y-%m-%d %H:%M:%S")
    
    return local_timestamp


# In[67]:


def add_backup_db_to_drive(drive_service):
    """
    adds the current database backup to google drive
    """
    
    formatted_date = str(datetime.now())
    formatted_date = ''.join([c for c in formatted_date if c.isdigit()])
    
    #folder id
    fid = '17S7wM_Jot5q7QK1CSvF7QADT760EBf9L'
    
    file_metadata = {
        'name': formatted_date + 'videos.db',
        'parents': [fid]
    }
    media = MediaFileUpload('videos_backup.db',
                            resumable=True)
    file = drive_service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()
    return file.get('id')


# In[68]:


def get_most_recent_video_stats(cur, last_ts):
    cur.execute('SELECT * FROM VideoStats WHERE Timestamp > "%s"'%(last_ts))
    new_videostats_result = cur.fetchall()
    
    cur.execute('SELECT DISTINCT(VideoID) FROM VideoStats WHERE Timestamp > "%s"'%(last_ts))
    new_video_ids = [item[0] for item in cur.fetchall()]
    
    cur.execute('SELECT DISTINCT(VideoId) FROM VideoStats WHERE Timestamp <= "%s"'%(last_ts))
    old_video_ids = [item[0] for item in cur.fetchall()]
    
    video_ids_to_add = [vid for vid in new_video_ids if vid not in old_video_ids]
    
    result_dict = {'new_videostats': new_videostats_result, 'vids_to_add': video_ids_to_add}
    
    return result_dict


# In[69]:


def get_most_recent_videos(cur, vids_to_add):
    
    vids_to_add = ['"%s"'%vid for vid in vids_to_add]
    vids_to_add = ','.join(vids_to_add)
    
    cur.execute('SELECT * FROM Videos WHERE VideoID IN (%s)'%(vids_to_add))
    new_videos_result = cur.fetchall()
    
    cur.execute('SELECT DISTINCT(UploadsID) FROM Videos WHERE VideoID IN (%s)'%(vids_to_add))
    new_uploads_ids = [item[0] for item in cur.fetchall()]
    
    cur.execute('SELECT DISTINCT(UploadsID) FROM Videos WHERE VideoID NOT IN (%s)'%(vids_to_add))
    old_uploads_ids = [item[0] for item in cur.fetchall()]
    
    uploads_ids_to_add = [uid for uid in new_uploads_ids if uid not in old_uploads_ids]
    
    result_dict = {'new_videos': new_videos_result, 'uploads_to_add': uploads_ids_to_add}
    
    return result_dict


# In[70]:


def get_most_recent_channels(cur, upload_ids_to_add):
    upload_ids_to_add = ['"%s"'%uid for uid in upload_ids_to_add]
    upload_ids_to_add = ','.join(upload_ids_to_add)
    
    cur.execute('SELECT * FROM Channels WHERE UploadsID IN (%s)'%(upload_ids_to_add))
    new_channels_result = cur.fetchall()
    
    result_dict = {'new_channels': new_channels_result}
    
    return result_dict


# In[71]:


def insert_videostats_into_backup(conn, cur, new_videostats):
    cur.executemany('INSERT INTO VideoStats VALUES (?,?,?,?,?,?)', new_videostats)
    conn.commit()
    return True


# In[72]:


def insert_videos_into_backup(conn, cur, new_videos):
    cur.executemany('INSERT INTO Videos VALUES (?,?,?,?,?,?,?,?)', new_videos)
    conn.commit()
    return True


# In[73]:


def insert_channels_into_backup(conn, cur, new_channels):
    cur.executemany('INSERT INTO Channels VALUES (?,?,?,?,?,?)', new_channels)
    conn.commit()
    return True


# # Run Code

# In[82]:


if __name__ == '__main__':
    
    try:
        #get the logging files
        backups_status_logs = open("youtube_backups_logging.txt", "a+")
        backups_error_logs = open("youtube_backups_error_logging.txt", "a+")

        #get the drive service
        drive_service = get_authenticated_drive_service()

        #get the email service
        gmail_service = get_authenticated_gmail_service()

        #get most recent timestamp from google drive
        last_ts = get_most_recent_created_time(drive_service)

        #create a database connection
        conn = sqlite3.connect('videos.db')
        cur = conn.cursor()

        #delete the current backup db file if it exists
        try:
            os.remove("videos_backup.db")
        except:
            pass

        #create the current backup file
        conn_backup = sqlite3.connect('videos_backup.db')
        cur_backup = conn_backup.cursor()

        #create the tables in this new database
        create_tables(conn_backup, cur_backup)

        #get most recent videostats
        result = get_most_recent_video_stats(cur, last_ts)
        new_videostats = result['new_videostats']
        vids_to_add = result['vids_to_add']

        #we only need to do work if there is something to add
        if len(new_videostats) > 0:

            #get most recent videos
            result = get_most_recent_videos(cur, vids_to_add)
            new_videos = result['new_videos']
            uploads_to_add = result['uploads_to_add']

            #get most recent channels
            result = get_most_recent_channels(cur, uploads_to_add)
            new_channels = result['new_channels']

            #insert the new video stats into the backup
            insert_videostats_into_backup(conn_backup, cur_backup, new_videostats)

            #insert the new videos into the backup
            insert_videos_into_backup(conn_backup, cur_backup, new_videos)

            #insert the new channels into the backup
            insert_channels_into_backup(conn_backup, cur_backup, new_channels)

            #upload the backup database to google drive
            new_file_id = add_backup_db_to_drive(drive_service)

            #send a success message for this backup
            my_email = 'ritvikmathematics@gmail.com'
            subject = 'Created Backup on %s'%str(datetime.now())
            body = "New Channels: %s\n\nNew Videos: %s\n\nNew VideoStats: %s\n\nFile ID: %s"                %(len(new_channels), len(new_videos), len(new_videostats), new_file_id) 
            success_email = create_message(my_email, my_email, subject, body)
            send_message(gmail_service, 'me', success_email)

            #write to success log
            backups_status_logs.write(body + '\n-----------------\n\n')

            print(new_file_id)

        #close connections to main db
        conn.close()

        #close connections to backup db
        conn_backup.close()

    except Exception as e:
        print(e)
        backups_error_logs.write(str(datetime.now()) + '\n\n' + str(e) + '\n-----------------\n\n')
        my_email = 'ritvikmathematics@gmail.com'
        subject = 'DB Backup Error on %s'%str(datetime.now())
        error_email = create_message(my_email, my_email, subject, str(e))
        send_message(gmail_service, 'me', error_email)

    finally:
        conn.close()
        conn_backup.close()
        backups_status_logs.close()
        backups_error_logs.close()