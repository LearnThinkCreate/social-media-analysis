import pandas as pd

from auth import spotify_authentication

sp = spotify_authentication()

def getSpotifyHistory():
    df = None 
    for i in range(3):
        data = pd.read_json(f'data/spotify/StreamingHistory{i}.json')
        try:
            df = df.append(data, ignore_index = True)
        except AttributeError:
            df = data
    return df

def cleanSpotifyData(data):
    df = pd.concat(
        objs = [
            # Cleaning the metadata
                pd.json_normalize(
                    data['items'],
                    None,
                    'album',
                    'album.',
                ).drop(
                    [
                        'artists', 'available_markets', 'is_local', 
                        'href', 'disc_number', 'popularity',
                        'track_number', 'preview_url', 'uri',
                        'album.artists', 'album.available_markets', 'album.external_urls.spotify',
                        'album.href', 'album.release_date_precision', 'album.total_tracks',
                        'album.type', 'album.uri', 'external_ids.isrc', 
                        'external_urls.spotify'
                        
                    ],
                    axis=1
                ),
            # Cleaning the artist data
            pd.json_normalize(
                data['items'],
                'artists', 
                record_prefix='artist.'
            ).drop(
                [
                    'artist.href', 'artist.type', 'artist.uri',
                    'artist.external_urls.spotify'
                ],
                axis=1
            )
        ],
        axis = 1
    )
    
    return df.head(50)

def callSpotify(func, id_list):
    df = pd.DataFrame()
    for id in id_list:
        raw_data = func(id)
        clean_data = pd.json_normalize(raw_data)
        # Appending the data to the DataFrame
        df = df.append(clean_data)
    return df