from flask import Flask, make_response, request
import json

app = Flask(__name__)

costs = {}

@app.route('/api/v1/lookup', methods=['POST'])
def get_costs():
    request_data = json.loads(request.data)
    batch = request_data['batch']
    tasks = request_data['tasks']

    response = {'batch': batch, 'costs': []}
    for task in tasks:
        if task['task_id'] in costs:
            response['costs'].append({'task_id': task['task_id'], 'node_costs': costs[task['task_id']]})

    return make_response(json.dumps(response))

@app.route('/api/v1/set', methods=['POST'])
def set_costs():
    global costs
    payload = json.loads(request.data)
    costs = payload

    return make_response('Updated cost data')
