import os
import pickle
import spotipy 

from spotipy.oauth2 import SpotifyOAuth
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

PICKLE_PATH = 'credentials/googleToken.pickle'

def spotify_authentication():
    """
    Authenticate with Spotify. If the credentials are cached then the 
    function will proceed without user input. Otherwise the user will
    be prompted to give conset in order to proceed.
    """
    SCOPE = [
        'user-read-playback-position',
        'user-read-recently-played',
        'user-top-read'
    ]

    authentication = SpotifyOAuth(
            client_id=os.environ.get('SPOTIFY_CLIENT'),
            client_secret=os.environ.get('SPOTIFY_SECRET'),
            redirect_uri='http://127.0.0.1:8000/callback',
            scope=SCOPE
        )

    sp = spotipy.Spotify(auth_manager=authentication)

    return sp


def get_google_credentials():
    """
    Retrives the google api credentials from the pickle file or writes a new one if none exist
    """
    credentials = check_google_credentials()

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            # The credentials file exist, but the refresh token is expired
            print('Refreshing Access Token...')
            credentials.refresh(Request())
        else:
            # The credentials file doesn't exist
            print('Fetching New Token....') 

            SCOPES = [
                "https://www.googleapis.com/auth/youtube.readonly",
                "https://www.googleapis.com/auth/docs",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
                ]
            # Get credentials and create an API client
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials/google_credentials.json", 
                scopes=SCOPES,
                redirect_uri="http://localhost:8000/"
                )

            flow.run_local_server(port=8000, prompt='consent')
            credentials = flow.credentials

        # Writing the credentials file
        with open(PICKLE_PATH,  'wb') as f:
            print('Saving Credentials for Future Use...')
            pickle.dump(credentials, f)

    return credentials


def check_google_credentials():
    """
    Checks if the credentials pickle file already exists. If it does
    then it returns the credentials, otherwise it returns None
    """
    credentials = None
    if os.path.exists(PICKLE_PATH):
        print('Loading Credntials From File...')
        with open(PICKLE_PATH, 'rb') as token:
            credentials = pickle.load(token)

    return credentials
    