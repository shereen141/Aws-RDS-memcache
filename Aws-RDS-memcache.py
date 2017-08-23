from random import randrange

from flask import Flask, render_template, request
import time
import pymysql
import os
import hashlib
import memcache

app = Flask(__name__)
cache = memcache.Client([], debug=0)

def connection():
    conn = pymysql.connect(host= ,
                           port= ,
                           user= ,
                           password= ,
                           db= ,
                           local_infile=True
                           )
    return conn


@app.route('/result1', methods=['POST', 'GET'])
def query():
    conn = connection()
    cur = conn.cursor()
    if request.method == 'POST':
        mytext = request.form['text1']
        mytext1 = request.form['text2']
        sql = 'select * from boat where place like "%' + mytext + '" and place like "%' + mytext1 + '" '

        cur.execute(sql)
        r = cur.fetchall()

    return render_template('result.html')

def createtable():
    start_time = time.time()
    print("in create table")
    conn = connection()
    cur = conn.cursor()
    cur.execute('drop table if exists boat2')
    conn.commit()

    query = "create table boat2 (pclass INT, survived INT,name VARCHAR(100) ,sex VARCHAR(6) ,age NUMERIC(7,4),ticket VARCHAR(18) , fare NUMERIC(7,4), cabin VARCHAR(15) , home_dest VARCHAR(100))"
    print("hi")
    cur.execute(query)
    load_file = app.root_path + '/boat.csv'
    query = """ LOAD DATA LOCAL INFILE 'E:/Uta/cloud/quiz4/boat.csv' INTO TABLE
                   boat2 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED
                   BY '"' Lines terminated by '\r\n' IGNORE 1 LINES """
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    end_time = time.time()
    print(end_time - start_time)
    return "hi done"


def dbcount():
    print('hi')
    conn = connection()
    cur = conn.cursor()
    quer = 'select count(*) from boat2'
    cur.execute(quer)
    res = cur.fetchone()
    #print(res[0])
    conn.commit()
    cur.close()
    conn.close()
    return res

############################### commiting query#########################
def commitingquery(query):
    conn = connection()
    cur = conn.cursor()
    sql=query;
    cur.execute(sql)
    value=cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return value


def memcache(query):
    conn = connection()
    cur = conn.cursor()

    hash = hashlib.sha256(query).hexdigest()
    key = "sql_cache:" + hash
    #print("key : %s" % key)
    if (cache.get(key)):
        #print("data is in cache")
        #print(cache.get(key))
        value=cache.get(key)
        return value
    else:
        cur.execute(query)
        value = cur.fetchall()
        cache.set(key, value, time=500)
        conn.commit()
        cur.close()
        conn.close()
        #print(cache.get(key))
        return value
################################## random query######################

def randomquery(mytext,mytext1,choice):
    mag1 = mytext
    mag2 = mytext1
    choice=choice
    r1 = randrange(int(mag1), int(mag2) - 1)
    r2 = randrange(int(r1) + 1, int(mag2) + 1)
    #print('r1', r1)
    #print('r2', r2)
    query = 'select longitude from all_month1 where mag BETWEEN "' + str(r1) + ' " and  " ' + str(r2) + '" '
    if choice=="memcache":
        result=memcache(query)
    else:
        result=commitingquery(query)

    return result


@app.route('/result', methods=['POST', 'GET'])
def callingfunc():
    start_time = time.time()
    #j = 0
    display=[]
    if request.method == 'POST':
        mytext = request.form['text1']
        mytext1 = request.form['text2']
        choice = request.form['text3']
        count = request.form['count']
        for i in range(1, int(count)):
            # print(j + 1)
            display.append(randomquery(mytext,mytext1,choice))
            #j += 1
    end_time = time.time()
    total_time=end_time - start_time
    print("final time:",total_time)
    display.append(total_time)
    return render_template('result.html', display=display)



#########################main
@app.route('/')
def hello_world():
    # createtable()
    var=dbcount()
    print("the count is", res)
    start_time = time.time()
    callingfunc()
     memcache()
    j = 0
    for i in range(1, 5001):
        print(j + 1)
        randomquery()
        j += 1
    end_time = time.time()
    print("final time:", end_time - start_time)
    return render_template('main.html')


def memcache_one():
    sample_obj = {"name": "Soliman",
                  "lang": "Python"}
    cache.set("sample_user", sample_obj, time=15)
    print("Stored to memcached, will auto-expire after 15 seconds")
    print(cache.get("sample_user"))


port = os.getenv('PORT', '80')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(port))