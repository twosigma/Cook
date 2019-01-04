from flask import Flask, make_response, request
import json

app = Flask(__name__)

submit_status = {'status': 'accepted', 'message': 'A message'}
launch_status = {'status': 'accepted', 'message': 'A message'}

@app.route('/get-submit-status', methods=['GET'])
def get_submit_status():
    return make_response(json.dumps(submit_status))

@app.route('/set-submit-status', methods=['POST'])
def set_submit_status():
    global submit_status
    submit_status = json.loads(request.data)
    print (f"Reset submit status to '{submit_status}'")
    return make_response('Updated submit status')

@app.route('/get-launch-status', methods=['GET'])
def get_launch_status():
    return make_response(json.dumps(launch_status))

@app.route('/set-launch-status', methods=['POST'])
def set_launch_status():
    global launch_status
    launch_status = json.loads(request.data)
    print (f"Reset launch status to '{launch_status}'")
    return make_response('Updated launch status')

