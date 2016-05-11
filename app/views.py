from flask import render_template, redirect, jsonify
from app import app

@app.route('/')
@app.route('/index')
def index():
	return render_template('/index.html')

@app.route('/volume/remove')
def volume_remove():
	return jsonify(result = "success")
