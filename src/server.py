from flask import Flask, send_from_directory 


from db import *
import schema 


app = Flask(__name__, static_url_path='/static_data/')

@app.route('/')
def hello():
  return "%s <br \> %s"%("/data/splices.json", "/data/traits.json")

@app.route('/data/<path:path>')
def serve_static(path):
    return send_from_directory('static_data', path)

if __name__ == '__main__':
    dba = DBAdapter()
    app.config['DEBUG'] = True

    import inspect 
    for (_, persistable) in inspect.getmembers(schema, inspect.isclass):
        create_crud_endpoints(persistable, app, dba)

    app.run(port=8081)
