from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import SQLAlchemy
from ETL import clean
import subprocess
import pandas as pd
import csv
#app = Flask(__name__,template_folder='template')
app = Flask(__name__,template_folder='Admin dashboard')

@app.route("/", methods=['GET','POST'])
def main():
    #return "Lan Anh nè"
    # return render_template('example.html', myvar='Flask')
    return render_template('index.html')

@app.route("/login")
def login():
    #return "<h1> You're on home page</h1>"
    return render_template('login.html')

@app.route("/process", methods=['GET','POST'])
def process():
    if request.method == "POST":

        if request.files:

            file_upload = request.files["file"]
            print('file uploaded')
            file_after=clean(file_upload)
            
            #return redirect(request.url)
            return file_after.to_html()
            
    return render_template('upload.html')

@app.route('/ETL')
def ETL():
    #import ETL
    #return DS_NK.to_html()
    return "hello"

if __name__ == "__main__":
    app.run(host='localhost',debug=True)
