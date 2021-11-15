from auth import *
from googleHelpers import *

# Connecting to Youtube Api
youtube = Youtube()

# Getting my Youtube History
history = youtube.scrapeRawHistory(VIDEO_HISTORY)

# Getting details on videos
videoIds = youtube.history.id.tolist()
videoDetails = youtube.callGoogle(youtube.getVideoDetails ,videoIds)

# Getting the categories of the vides
categoryIds = videoDetails.categoryId.unique().tolist()
cat = youtube.callGoogle(youtube.getCategoryDetails, categoryIds)

# Getting the full history
history = videoDetails.merge(
    cat,
    "left",
    on='categoryId'
).drop_duplicates().reindex()

history.to_csv('data/fullYouTubeHistory.csv')