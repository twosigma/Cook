from flask import Flask, make_response, request
import json

app = Flask(__name__)

submit_status = {'status': 'rejected', 'message': 'A message'}
launch_status = {'status': 'accepted', 'message': 'A message'}

@app.route('/get-submit-status', methods=['GET'])
def get_submit_status():
    return make_response(json.dumps(submit_status))

@app.route('/set-submit-status', methods=['POST'])
def set_submit_status():
    global submit_status
    payload = json.loads(request.data)
    submit_status = payload
    return make_response('Updated cost data')

@app.route('/get-launch-status', methods=['GET'])
def get_launch_status():
    return make_response(json.dumps(launch_status))

@app.route('/set-launch-status', methods=['POST'])
def set_launch_status():
    global launch_status
    payload = json.loads(request.data)
    launch_status = payload
    return make_response('Updated cost data')

