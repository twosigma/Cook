from flask import Flask, request, jsonify
import logging

logger = logging.getLogger('optimizer')
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level = 'INFO')

app = Flask(__name__)


@app.route('/')
def hello():
    return "Hello World!"


@app.route('/optimizer', methods = ['GET', 'POST'])
def optimizer():
    logger.info('Received request to optimizer, extracting job uuids')
    content = request.get_json()
    job_uuids = [job["uuid"] for job in content["opt_jobs"]]
    logger.info('Extracted jobs ids, count = ' + str(len(job_uuids)))
    response = {"schedule": {"0": {"suggested-matches": {"0": job_uuids}}}}
    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9095')