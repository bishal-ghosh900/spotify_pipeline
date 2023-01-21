import logging as log

from utils import recently_played_songs_json
from utils import covert_to_dataframe
from utils import remove_missing_data
from utils import remove_duplicate_data
from utils import connect_to_mysql
from utils import load_dataframe_to_mysql

# setting the logging level to INFO
log.basicConfig(level=log.INFO)

if __name__ == '__main__':

    try:
        log.info('fetching the songs...')
        recent_played_song_json = recently_played_songs_json()
        log.info('fetching completed')

        log.info('loading the songs data to dataframe...')
        recently_played_songs = covert_to_dataframe(recent_played_song_json['items'])
        log.info('loading completed')

        log.info('removing records with nulls from the dataframe...')
        recently_played_songs = remove_missing_data(recently_played_songs) 

        log.info('removing duplicate data from the dataframe...')
        recently_played_songs = remove_duplicate_data(recently_played_songs) 

        log.info('connecting to the mysql database...')
        mysql_con = connect_to_mysql()
        log.info('connection completed')

        log.info('loading the dataframe data to mysql table...')
        load_dataframe_to_mysql(
            df = recently_played_songs, 
            con = mysql_con, 
            table_name='recently_played_songs',
        )
        log.info('loading completed')


    except Exception as e:
        log.error(e.args[0])


    
