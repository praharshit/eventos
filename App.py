import os
import json
from flask import Flask, request, jsonify, Response, render_template, send_from_directory
from service.index_search import Search
from service.es_index import EsIndex, EsSearch

app = Flask(__name__)
# search_instance = Search()
es_index = EsIndex()
es_search = EsSearch("test_v1")

jinja_options = app.jinja_options.copy()

jinja_options.update(dict(
    block_start_string='<%',
    block_end_string='%>',
    variable_start_string='%%',
    variable_end_string='%%',
    comment_start_string='<#',
    comment_end_string='#>'
))
app.jinja_options = jinja_options


@app.route('/')
def home():
    return render_template('home.html')

@app.route('/ui_lib/<path:filename>', methods=['GET'])
def serve_static(filename):
    """
    To serve static files for the UI demo page
    :param filename: UI Resource path
    :return:
    """
    curr_dir = os.getcwd()
    return send_from_directory(os.path.join(curr_dir, 'ui_lib'), filename)

@app.route('/search', methods=['POST'])
def search():
    requestBody = json.loads(request.data)
    print((requestBody['query']))
    results = es_search.search(requestBody['query'])
    return jsonify({"events": results})

@app.route('/index', methods=['POST'])
def index():
    es_index.index_mongo_data()
    return jsonify({"status": "Success"})

if __name__ == '__main__':
    app.run(debug=True)
