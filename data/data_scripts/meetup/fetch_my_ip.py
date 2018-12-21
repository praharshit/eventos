
from flask import request
from flask import jsonify
from flask import Flask
import requests
import json

###
#
# authors: Akshay
#
###

app = Flask(__name__)

@app.route("/get_my_ip", methods=["GET"])
def get_my_ip():
    print("Request received from ip:", request.remote_addr)
    ip_location_url = "http://api.ipstack.com/"
    access_key = "?access_key=564a1bf49449e3adc2985ee9bca32382"

    url = ip_location_url + request.remote_addr + access_key
    print(url)

    response = requests.get(url)
    j = response.text
    data = json.loads(j)

    resp = jsonify(
        {'ip': request.remote_addr, 'zipcode': data['zip'], 'lat': data['latitude'], 'lon': data['longitude'],
         'city': data['city']})
    print(resp)
    return resp


@app.route('/')
def index():
    return 'Hello World'

if __name__ == "__main__":
    app.run(host='0.0.0.0')