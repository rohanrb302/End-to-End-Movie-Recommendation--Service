import json

from flask import Flask, Response

from app.database import Database
from app.recommend import Recommend

app = Flask('Movie-Recommendation-System')


@app.route('/')
def healthcheck():
    return 'Server UP!'


@app.route('/recommend/<userid>')
def recommend(userid):
    if userid.isnumeric() and int(userid) >= 0:
        return service.recommend(int(userid))
    return Response(status=406)


@app.teardown_appcontext
def teardown(exception):
    db.teardown()


if __name__ == '__main__':
    with open('config/config.json') as cfg:
        conf = json.load(cfg)

    db = Database(conf['rds_db'])
    s3 = conf['aws_access']

    model_conf = conf['model']
    service = Recommend(
        model_conf['recommendations_path'],
        model_conf['mappings_path'],
        model_conf['popular_movie_path'],
        db,
        s3
    )

    app.run(host=conf['flask']['host'], port=conf['flask']['port'], debug=False)
