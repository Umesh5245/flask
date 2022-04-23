import collections
from collections import OrderedDict

from flask import Flask, render_template, request, redirect, url_for
import os
from datetime import datetime

import pandas as pd
import mysql.connector

app = Flask(__name__)

app.config["DEBUG"] = True

UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="csvdata"
)

mycursor = mydb.cursor()

mycursor.execute("SHOW DATABASES")

# View All Database
for x in mycursor:
  print(x)





# Root URL
@app.route('/')
def index():
     # Set The upload HTML template '\templates\index.html'
    return render_template('index.html')
@app.route('/mysql')
def mysql1():
     # Set The upload HTML template '\templates\index.html'
     if (request.method == 'POST'):
        parseCSV("D:\download\IPL Matches 2008-2020 (3).csv")

     return redirect(url_for('mysql2'))

@app.route("/mysql1", methods=['POST'])
def mysql2():
    parseCSV("D:\download\IPL Matches 2008-2020 (3).csv")
    return render_template('mysqlresult.html')
@app.route("/mysql2", methods=['POST'])
def mysql2():
    parseCSV("D:\download\IPL Matches 2008-2020 (3).csv")
    return render_template('mysqlresult.html')


# Get the uploaded files
@app.route("/", methods=['POST'])
def uploadFiles():
    # if(request.method=='POST'):
    #   parseCSV("D:\download\IPL Matches 2008-2020 (3).csv")
      return redirect(url_for('mysql1'))

def parseCSV(filePath):
      deleteData()
      insertdata(filePath)
      playerofmatch()
      TopVenue()
      ElectedField()


def deleteData():
    query1 = "delete from csv"
    mycursor.execute(query1)


def ElectedField():
    query2 = "SELECT toss_winner FROM csv where toss_decision ='field' AND date>='2016-01-01' AND DATE<='2017-12-31'"
    mycursor.execute(query2)
    rowss = mycursor.fetchall()
    emptyDictt = dict()
    for row in rowss:
        if row[0] in emptyDictt:
            emptyDictt[row[0]] = emptyDictt[row[0]] + 1
        else:
            emptyDictt[row[0]] = 1
    print(emptyDictt)
    tempp = sorted(emptyDictt, key=emptyDictt.get, reverse=True)
    for x in (0, 1, 2, 3):
        print(str(tempp[x]) + " " + str(emptyDictt[tempp[x]]))


def TopVenue():
    mycursor.execute("SELECT * FROM csv ")
    rows = mycursor.fetchall()
    emptyDict = dict()
    for row in rows:
        print(row)
        tt = row[9]
        ty = row[7]
        if ((ty) == (tt)):
            if (row[8] == 'bat'):
                if row[9] in emptyDict:
                    emptyDict[row[9]] = emptyDict[row[9]] + 1
                else:
                    emptyDict[row[9]] = 1
        elif (row[9] != row[7]):
            if (row[8] == 'field'):
                if row[9] in emptyDict:
                    emptyDict[row[9]] = emptyDict[row[9]] + 1
                else:
                    emptyDict[row[9]] = 1
    tempp = sorted(emptyDict, key=emptyDict.get, reverse=True)
    for x in (0, 1, 2, 3):
        print(str(tempp[x]) + " " + str(emptyDict[tempp[x]]))


def playerofmatch():
    query1 = "SELECT player_of_match FROM csv GROUP BY player_of_match ORDER BY COUNT(player_of_match) DESC LIMIT 1 "
    mycursor.execute(query1)
    cnt = mycursor.fetchall()
    print("most number of player of match", cnt)


def insertdata(filePath):
    col_names = ['id', 'city', 'date', 'player_of_match', 'venue', 'neutral_venue', 'team1', 'team2', 'toss_winner',
                 'toss_decision', 'winner', 'result', 'result_margin', 'eliminator', 'method', 'umpire1', 'umpire2']
    csvData = pd.read_csv(filePath, names=col_names, header=None)
    csvData.fillna(0)
    for i, row in csvData.iterrows():
        sql = "INSERT INTO csv (id,city,date,player_of_match,venue,neutral_venue,team1,team2,toss_winner,toss_decision,winner,result,result_margin,eliminator,method,umpire1,umpire2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        if (pd.isna((row['method']))):
            row['method'] = 'NONE'
        if (pd.isna((row['result_margin']))):
            row['result_margin'] = 'NONE'
        if (pd.isna((row['player_of_match']))):
            row['player_of_match'] = 'NONE'
        if (pd.isna((row['winner']))):
            row['winner'] = 'NONE'
        if (pd.isna((row['result']))):
            row['result'] = 'NONE'
        if (pd.isna((row['eliminator']))):
            row['eliminator'] = 'NONE'
        if (pd.isna((row['city']))):
            row['city'] = 'NONE'
        print(row['date'])
        try:
            the_date = datetime.strptime(
                row['date'],
                '%Y-%m-%d')
        except:
            continue

        value = (
        row['id'], row['city'], the_date, row['player_of_match'], row['venue'], row['neutral_venue'], row['team1'],
        row['team2'], row['toss_winner'], row['toss_decision'], row['winner'], row['result'], row['result_margin'],
        row['eliminator'], row['method'], row['umpire1'], row['umpire2'])
        if (pd.isna(row['id'])):
            continue
        print(value)
        mycursor.execute(sql, value)
        mydb.commit()


if (__name__ == "__main__"):
     app.run(port = 5000)