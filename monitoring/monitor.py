from prometheus_client import Counter, Histogram
import json
import signal
from kafka import KafkaConsumer

from prometheus_client import start_http_server

start_http_server(5000)

REQUEST_COUNT = Counter(
    'request_count', 'Recommendation Request Count',
    ['http_status']
)

REQUEST_LATENCY = Histogram('request_latency_seconds', 'Request latency')

def read_configs(config_path):
    '''
    Read configs from JSON
    '''

    kafkaConf = None
    with open(config_path) as cfg:
        configs = json.load(cfg)
        kafkaConf = configs['kafka']

    return kafkaConf


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


def monitor_metrics(consumer, killswitch):
    # Start consuming from Kafka topic
    for message in consumer:
        # Get the timestamp in BIGINT
        ts = message.timestamp

        # Read message byetes into comma-separated string
        event = message.value.decode('utf-8')

        # Parse values from event
        values = event.split(',')
        # print(values)
        if 'recommendation request' in values[2]:
            status = values[3].strip().split(" ")[1]
            time_taken = float(values[-1].strip().split(" ")[0])
            # print(status, time_taken)
            REQUEST_COUNT.labels(status).inc()
            REQUEST_LATENCY.observe(time_taken / 1000)

        if killswitch:
            break


def cleanup(consumer):
    consumer.close()


def toggle_killswitch(signalNumber, frame):
    global killswitch
    killswitch = True

def setup_metrics():
    killswitch = False
    signal.signal(signal.SIGTERM, toggle_killswitch)

    # Read configs
    config_path = "config.json"
    kafkaConf = read_configs(config_path)

    # Connect to Kafka
    consumer = connect_kafka(kafkaConf)

    # Set pointer to end of partition
    consumer.poll()
    consumer.seek_to_end()

    monitor_metrics(consumer, killswitch)

if __name__ == "__main__":
    # Setup killswitch for clean exit
    killswitch = False
    signal.signal(signal.SIGTERM, toggle_killswitch)

    # Read configs
    config_path = "config.json"
    kafkaConf = read_configs(config_path)

    # Connect to Kafka
    consumer = connect_kafka(kafkaConf)

    # Set pointer to end of partition
    consumer.poll()
    consumer.seek_to_end()

    monitor_metrics(consumer, killswitch)

    # Cleanup after breaking loop
    cleanup(consumer)