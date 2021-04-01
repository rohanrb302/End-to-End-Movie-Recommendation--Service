import json
import signal

import sqlalchemy
from kafka import KafkaConsumer
from scanf import scanf

def read_configs(config_path):
    '''
    Read configs from JSON
    '''

    dbConf = None
    kafkaConf = None
    with open(config_path) as cfg:
        configs = json.load(cfg)
        dbConf = configs['db']
        kafkaConf = configs['kafka']

    return dbConf, kafkaConf


def connect_db(dbConf):
    '''
    Setup DB connection
    '''

    # Build connection URL
    dburl = 'postgresql://{}:{}@{}:{}/{}'.format(
        dbConf['username'],
        dbConf['password'],
        dbConf['host'],
        dbConf['port'],
        dbConf['db_name']
    )

    # Create DB connection
    con = sqlalchemy.create_engine(dburl, client_encoding='utf8')

    # Build metadata
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta


def connect_kafka(kafkaConf):
    '''
    Setup Kafka broker connection
    (NOTE: SSH Tunnel to brokers needs to be running at the time of execution)
    '''
    consumer = KafkaConsumer(
        kafkaConf['topic_name'],
        bootstrap_servers=kafkaConf['bootstrap_servers'],
        auto_offset_reset='earliest',
        group_id=kafkaConf['group_id'],
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    return consumer


def evaluate_ratings(recommendations_table, rating_event_table, consumer, db_con,model_id=1):
    # Start consuming from Kafka topic
    for message in consumer:
        # Get the timestamp in BIGINT
        ts = message.timestamp

        # Read message byetes into comma-separated string
        event = message.value.decode('utf-8')

        # Parse values from event
        values = event.split(',')
        if(len(values) == 3 and '/rate/' in values[2]):
            user_id = values[1]
            movie_id, rating = scanf('GET /rate/%s=%s', values[2])
            year = movie_id.split('+')[-1]
            check_valid=validate(ts, user_id, movie_id, rating, year)
            if check_valid:
                # Insert into database table
                user = int(user_id)
                rating = int(rating)

                insert_clause = rating_event_table.insert().values(
                    kafka_ts=ts,
                    user_id=user_id,
                    movie_id=movie_id,
                    rating=rating,
                    model_id =model_id
                )
                db_con.execute(insert_clause)
                # print("Inserted")

                user_liking = get_user_liking(rating)

                update_clause = recommendations_table.update().where(
                    (recommendations_table.c.user_id == user_id) &
                    (recommendations_table.c.movie_id == movie_id) & 
                    (recommendations_table.c.rating == 0) ).values({'rating':user_liking,'model_id':1})
                db_con.execute(update_clause)

        if killswitch:
            break

def get_user_liking(rating):
    user_liking = 1 if rating >= 3 else -1

    return user_liking

def validate_timestamp(timestamp):
    if (timestamp > 0 and type(timestamp)==int):
        return True
    
    return False

def valiate_year(year):
    try:
        year=int(year)
        if(year <= 2020 and year > 1800):
            return True
        
        return False
    except:
        return False
  

def validate_rating(rating):
    try:
        rating = int(rating)
        if(type(rating) == int and  rating >=1 and rating <=5):
            return True
        
        return False
    except:
        return False

def validate_userid(user_id):
    try:
        user_id=int(user_id)
        if(type(user_id)==int and user_id > 0):
            return True
        
        return False
    except:
        return False

    

def validate(timestamp, user_id, movie_id, rating, year):
    '''
    # Set data type validations 
    # Range of ratings from 1 to 5
    # Check if the movie_id is valid
    # Check if the user id is valid
    '''
    try:
        if(valiate_year(year) and validate_timestamp(timestamp) and validate_rating(rating) and validate_userid(user_id)):
            return True
    
        return False

    except:
        return False


def cleanup(consumer, db_con):
    consumer.close()
    db_con.dispose()


def toggle_killswitch(signalNumber, frame):
    global killswitch
    killswitch = True


if __name__ == "__main__":
    # Setup killswitch for clean exit
    killswitch = False
    signal.signal(signal.SIGTERM, toggle_killswitch)

    # Read configs
    config_path = "/home/harshjai/movie-prediction-group-project-jurassicpark/confs/config.json"
    dbConf, kafkaConf = read_configs(config_path)

    # Connect to DB
    con, meta = connect_db(dbConf)

    # Get table from database
    recommendations_table = meta.tables['recommendations']
    rating_event_table = meta.tables['rating_event']

    # Connect to Kafka
    consumer = connect_kafka(kafkaConf)

    # Set pointer to end of partition
    consumer.poll()
    consumer.seek_to_end()

    # Begin online evaluation
    evaluate_ratings(recommendations_table, rating_event_table, consumer, con)

    # Cleanup after breaking loop
    cleanup(consumer, con)
