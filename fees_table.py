import requests
import tabulate
import json
from datetime import datetime
import argparse
import sys

def print_table(data, tag, sort_by):
    print(f'Formatting subgraph {tag} output.')
    headers = [*data[0]]
    table = []
    data = sorted(data, key=lambda k: k[sort_by])
    for line in data:
        row = []
        for key in [*line]:
            row.append(line[key])
        table.append(row)
    try:
        print(tabulate.tabulate(table, headers, tablefmt="fancy_grid"))
    except UnicodeEncodeError:
        print(tabulate.tabulate(table, headers, tablefmt="grid"))

def load_data(file_name):
    data = []
    try: 
        with open(file_name) as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
            print('File not accisable')
    return data

def save_data(data, file_name):
    print("Saving the data to file {file_name}")
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file)

def Requst_data(URL, REQUEST_QUERY):
    #URL = "https://gateway-testnet.thegraph.com/network"
    #REQUEST_QUERY = {'query': '{ indexers   { id queryFeesCollected queryFeeRebates} }'}
    try:
        response = requests.post( URL, json=REQUEST_QUERY )
    except requests.exceptions.RequestException as e:
        sys.exit("Network Failure")
    try:
        result = response.json()['data']['indexers']
    except ValueError:
        sys.exit("Network Failure")
    return result

def find_id(data, id_):
    _time = datetime.now()
    _timestamp = datetime.timestamp(_time)
    for line in data:
        if line['id'] == id_:
            line ['time'] = _time.strftime("%d/%m/%Y %H:%M:%S")
            line['timestamp']= _timestamp
            return line
    return {'time': _time.strftime("%d/%m/%Y %H:%M:%S"),'timestamp': _timestamp, "id": "Unknow", "queryFeesCollected": 0, "queryFeeRebates": 0}

def delta_data(data):
    counter = -1
    result = []
    for line in data:
        if counter == -1:
            time_delta = 0
            queryFeesCollected_delta = 0
            queryFeeRebates_delta = 0
            fees_rate = 0
        else:
            time_delta = (line['timestamp'] - data[counter]['timestamp']) / 60
            queryFeesCollected_delta = (int(line['queryFeesCollected']) - int(data[counter]['queryFeesCollected'])) / 1000000000000000000
            queryFeeRebates_delta = ( int(line['queryFeeRebates']) - int(data[counter]['queryFeeRebates'])) / 1000000000000000000
            fees_rate = queryFeesCollected_delta / time_delta
        delta_dic = {'Time': line['time'], 'Time Delta (mins)': time_delta, 'Query Fees Collected': queryFeesCollected_delta, 'Fees Rate': fees_rate, 'Query Fee Rebates': queryFeeRebates_delta}
        result.append(delta_dic)
        counter += 1
    return result

def process_data(_id, hist_len):
    config = load_data('.config.json')
    #query =  {'query': '{ indexers { id queryFeesCollected queryFeeRebates} }'}
    query = { 'query': config[0]['query']}
    url = config[0]['URL']
    file_name = 'fees'+'_'+_id+'.json'
    data = Requst_data(url, query)
    id_data = find_id(data, _id)
    master_data = load_data(file_name)
    master_data.append(id_data)
    while len(master_data) > hist_len:
        master_data.pop(0)
    _delta_data = delta_data(master_data)
    print_table(_delta_data, 'Fees Data',  'Time')
    save_data(master_data,file_name)

def get_args():
    my_parser = argparse.ArgumentParser(description='Get the Delta of income, last 20')
    my_parser.add_argument('id',metavar='id',type=str,help='ID to query')
    args = my_parser.parse_args()
    return args.id

if __name__ == "__main__":
    #_id = '0x6c0fadd48e7e236bb10f7d69148be5502a18ca57'
    _id = get_args()
    process_data(_id, 20)
