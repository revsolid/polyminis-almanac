from flask import Flask, send_from_directory 
app = Flask(__name__, static_url_path='/static_data/')

@app.route('/data/<path:path>')
def send_js(path):
    return send_from_directory('static_data', path)

if __name__ == '__main__':
    app.run()
