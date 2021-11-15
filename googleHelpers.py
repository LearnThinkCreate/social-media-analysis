import time 
import datetime
import urllib.parse
import math
import pandas as pd

from bs4 import BeautifulSoup
from auth import *

VIDEO_HISTORY = 'data/watch-history.html'
SEARCH_HISTORY = 'data/search-history.html'

class Google():
    def __init__(self):
        self.googleObject = None

    def callGoogle(self,  func, id_list):
        """
        This is a wrapper function for higher level api functions defined in 
        child classes

        Google API enpoints only take a maximum of 50 ids at once in API calls, so in order to go through thousands of 
        youtube videos or documents multiple API calls are needed.

        This function will make all the most effiecent number of API calls and combine all of the data into a pandas DataFrame
        """
        n = math.ceil(len(id_list) / 50)
        start = 0
        end = 50
        df = pd.DataFrame()

        for i in range(n):
            # Validating end index
            max_index = (i == (n - 1))

            # Setting the last index to the final value of the list to avoid a indexError      
            if max_index:
                end = len(id_list) - 1            

            # Converting list to a comma seperated string
            ids = ','.join(id_list[start:end])
            
            meta = {'start':start,'end':end}
            # Calling api
            data = func(ids, meta=meta)
                   
            # Appending the data to the DataFrame
            df = df.append(data)
                            
            # Adding to indexes for next iter
            if not max_index:
                start += 50
                end += 50

        return df

class Youtube(Google):

    def __init__(self):
        self.googleObject = build('youtube', 'v3', credentials=get_google_credentials())

    @property
    def searchHistory(self):
        if os.path.exists('data/youtubeSearchHistory.csv'):
            searchHistory = pd.read_csv('data/youtubeSearchHistory.csv')
            return searchHistory
        searchHistory = self.scrapeRawHistory('data/youtubeHistory', type = "search")
        return searchHistory

    @property
    def videoHistory(self):
        if os.path.exists('data/youtubeHistory.csv'):
            videoHistory = pd.read_csv('data/youtubeHistory.csv')
            return videoHistory
        videoHistory = self.scrapeRawHistory('data/youtubeHistory', type = "video")
        return videoHistory

    def getVideoDetails(self, video_ids, meta=None):
        """
        To be run as an argument to the callGoogle function. 

        Takes a string of YouTube video Ids and returns the details of those videos
        """
        results = self.googleObject.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=video_ids
                ).execute()

        data = pd.json_normalize(
            results['items']
          )[[
              "id",  'snippet.title', "snippet.channelTitle",
              "snippet.categoryId",  "snippet.tags", 'snippet.description', 
               'contentDetails.duration', "snippet.publishedAt", 'statistics.viewCount',
               'snippet.channelId', "snippet.liveBroadcastContent"
          ]].rename(columns={
            "snippet.channelTitle":'channelTitle',
            "snippet.tags":'tags',
            "snippet.categoryId":"categoryId",
            "snippet.publishedAt":"publishDate",
            "snippet.channelId":'channelId',
            "snippet.title":"videoTitle",
            "snippet.description":"description",
            "contentDetails.duration":"duration",
            "statistics.viewCount":'viewCount',
            "snippet.liveBroadcastContent":'liveContent'
          })

        # Cleaning the tags
        
        # Pulling tags from the df
        df = data[['tags', 'id']]
        data = data.drop(columns=['tags'])
        # Initalizing a set and pulling all the tags
        unique_tags = set()
        tags = df.tags.tolist()
        for i in range(len(df)):
            tag = tags[i]
            if isinstance(tag, list):
                tag = ",".join(tag)

            pair = (df.id.tolist()[i], tag)
            unique_tags.add(pair)

        unique_tags = pd.DataFrame(list(unique_tags), columns = ['id', 'tags'])

        data = data.merge(
            unique_tags,
            how='left',
            on='id'
        )

        # Adding timestamps from the history file
        if meta:
            try:
                history = self.history
            except AttributeError:
                history = self.scrapeRawHistory()
            try:
                data = data.merge(
                    history[meta['start']:meta['end']],
                    how='left',
                    on='id'
                )
            except Exception as e:
                print(e)

        return data
    
    def getCategoryDetails(self, video_ids, meta=None):
        results = self.googleObject.videoCategories().list(
            part='snippet',
            id=video_ids
        ).execute()

        data = pd.json_normalize(
            results['items']
        )[[
            'id', 'snippet.title'
        ]].rename(columns={'snippet.title':'categoryTitle', 'id':"categoryId"})

        return data

    def scrapeRawHistory(self, file, type = "video"):
        """
        Scrapes the youtube history file from Google Takeout
        https://takeout.google.com
        """ 
        start = time.time()
        with open(file) as f:
            soup = BeautifulSoup(f, 'html.parser')

        history_container = soup.body.div

        history = []
        # Finding container 
        for container in history_container:
            content = container.div.find('div', class_='mdl-typography--body-1')

            data = {}
            try:
                if type == "video":        
                    data['time'] = datetime.datetime.strptime(content.contents[3], '%b %d, %Y, %H:%M:%S %p %Z''')
                    data['status'] = str(content.contents[0].strip())
                    data['id'] = urllib.parse.urlparse(str(content.a.contents[0])).query.split('=')[1]
                    history.append(data)
                elif type == "search":
                    data['time'] = datetime.datetime.strptime(content.contents[3], '%b %d, %Y, %H:%M:%S %p %Z''')        
                    data['query'] = content.contents[1].contents[0]          
            except (IndexError, AttributeError, TypeError):
                pass

        end = time.time()

        print(f'{len(history)} videos loaded in {end - start} seconds')

        if type == "video":
            history = pd.DataFrame(history)[['time','id']].rename(columns={'time':'timestamp'})
            return history
        elif type == "search":
            history = pd.DataFrame(history)[['time','query']].rename(columns={'time':'timestamp'})
            return history
