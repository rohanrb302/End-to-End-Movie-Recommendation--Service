CREATE TABLE rating_event (
    event_id SERIAL PRIMARY KEY,
    kafka_ts BIGINT NOT NULL,
    user_id INT NOT NULL,
    movie_id VARCHAR ( 255 ) NOT NULL,
    rating INT NOT NULL
);

CREATE TABLE recommendations (
    user_id INT NOT NULL,
    movie_id VARCHAR ( 255 ) NOT NULL,
    rating INT NOT NULL,
    PRIMARY KEY(user_id, movie_id)
);