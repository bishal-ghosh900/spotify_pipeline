# spotify_pipeline

- Here I am using spotify api.
- I am using the api which gives data about recently played songs.
- All of the songs which I listened from yesterday (24 hours ago) till now will be fetched.
- Extraction of data is completed.
- Fetched data then going to be loaded into pandas dataframe.
- Now using pandas, those data will be cleaned (mainly date related cleaning).
- I am using played_at attribute as the primary key
- So validation of duplicate record is also happening based on this played_at attribute
- I alse have checked for null. If any record contains null then just removing it.
- Transformation of data is completed.
- I am using MySQL to load the transformed data
- To load the data I am using pandas to_sql method
- Attributes are: track, artist, album, played_at
- Loading of data is completed
