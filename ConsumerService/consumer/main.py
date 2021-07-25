# aio_pika template shamelessly stolen from: https://aio-pika.readthedocs.io/en/latest/quick-start.html

import json
import asyncio
import json
import os
import datetime
import iso8601 # datetime.fromiso() failed parsing the json examples. iso8601 lib to the rescue!

import aiopg # async postgres wrapper

from aio_pika import connect_robust, IncomingMessage
from flask import Flask, jsonify, request

PG_HOST = 'localhost' # wasn't sure how to use the local PGDATA env. varaible, so assuming localhost
dbconstr='dbname=' + os.environ.get('POSTGRES_DB') + ' user=' + os.environ.get('POSTGRES_USER') + ' password=' + os.environ.get('POSTGRES_PASSWORD') + ' host=' + PG_HOST 
pgcon=0 # will be initialized with DB connection

tablename='raw_events'
maxMedNameLen = 255

# to improve: move class to seaprate file
class EventManager:
    def process_start(self, e):
        if self.startsToCancel > 1:
            self.startsToCancel -= 1
        elif len(self.stops) > 0:
            # this start event was not cancelled in the future, and there is a matching (uncanceled) stop event.
            # append a period with start time (specified in e), and end time (latest time added to self.stops)
            self.periods.append((e['event_time'],self.stops.pop()))
    def process_stop(self, e):
        if self.stopsToCancel > 0:
            self.stopsToCancel -= 1
        else:
            self.stops.append()
    def process_cancel_start(self, e):
        self.startsToCancel=self.startsToCancel+1
        return
    def process_cancel_stop(self, e):
        self.stopsToCancel=self.stopsToCancel+1
        return
    
    action_list = ['start', 'stop', 'cancel_start', 'cancel_stop'] # used as a "static" list
    action_operation={action_list[0] : process_start, action_list[1]: process_stop, action_list[2]: process_cancel_start, action_list[3]:process_cancel_stop}
    def __init__(self):
        self.periods = [] # will include tuples: (start datetime, end datetime)
        self.startsToCancel=0
        self.stopsToCancel=0
        self.stops=[]

    def processEventSequence(self, sequence):
        """
        sequence is an event list, sorted by descending event time. All events refer to the same medication

        implementation assumptions:    
        1) cancel requests may be given in any order, and each cancel_start(cancel_stop) will cancel the latest start(stop)
        that came before it, even if there were stop(start) and/or cancel_stop(cancel_start) events between them.
        2) if there was an unpaired event (could be any of start, stop, cancel_start, cancel_stop), ignore it
        3) the event time in cancel_stop and cancel_start doesn't indicate the specific event to cancel, namely the
        time should be ignored
        """
        for e in sequence:
            self.action_operation[e['action']](e)

async def process_message(message: IncomingMessage):
    async with message.process():
        ev = json.loads(message.body)
        
        try:
            uid = int(ev['p_id'])
            ttime = iso8601.parse_date(ev['event_time'])
            medname = ev['medication_name']
            action = ev['action']
            
            if len(medname) >= maxMedNameLen or not (action in EventManager.action_list):
                return # dont requeue message
            
        except: # event is missing a field or mismatching format. no point re-queueing the message:
            return

        sql_query = "INSERT INTO %s (p_id, event_time, action, medication_name) VALUES (%s,%s,%s,%s)"
        # before executing, sanitize user_input (e.g. to avoid SQL injections). 
        # stored-procedures/psycopg2.sql string-composition are still better, but I assume its outside scope of this exercise
        insertRes = aiopg.Cursor.execute(sql_query, (tablename,str(uid),ttime.strftime('%Y-%m-%d %H:%M:%S'),action,medname))
        await insertRes

async def initRMQ(loop):
    # connect_robust also auto-recovers from connection problems (as opposed to connect() )
    connection = await connect_robust(host=os.environ.get('RABBIT_HOST'),
                               login=os.environ.get('RABBIT_USER'),
                               password=os.environ.get('RABBIT_PASS'),
                               loop=loop
                               )
    # Creating channel
    channel = await connection.channel()

    # process at most 8 events simultaneously
    await channel.set_qos(prefetch_count=8)

    # Declaring queue
    queue = await channel.declare_queue('events', auto_delete=True)

    await queue.consume(process_message, no_ack=False) # if process fails, we want it to get re-queued

    return connection


async def prepDBTable(conn):
    
    await conn.cursor().execute("DROP TABLE IF EXISTS " + tablename)
    # TO IMPROVE: move query below to separate file/stored procedure
    await conn.cursor().execute("CREATE TABLE " + tablename + " (p_id int, event_time TIME, action varchar(13), medication_name varchar(" + str(maxMedNameLen) + "), CONSTRAINT all_pk PRIMARY KEY (p_id,event_time,action,medication_name))")            
    
async def main(loop):
    async with aiopg.create_pool(dbconstr) as pool:        
        async with pool.acquire() as conn:
            #await prepDBTable(conn)
            async with conn.cursor() as cur:
                # creates/overrides table for storing events:
                # (TO IMPROVE: move query below to separate file/stored procedure)
                await cur.execute("DROP TABLE IF EXISTS " + tablename)
                await cur.execute("CREATE TABLE " + tablename + " (p_id int, event_time TIME, action varchar(13), medication_name varchar(" + str(maxMedNameLen) + "), CONSTRAINT all_pk PRIMARY KEY (p_id,event_time,action,medication_name))")            

app = Flask(__name__)

# interface will provide medication periods for: p_id given in GET
@app.route('/periods', methods=['GET'],)
async def get_tasks():
    try:
        pid=request.args.get('p_id')
        events = []
        pgc = pgcon.cursor()
        await pgc.execute("SELECT p_id=%s FROM " + tablename + " ORDER BY medication_name, event_time DESC", (pid))
        async for row in pgc:
            events.append[row]
        
        # events will list all raw events for the user. 
        # we get results grouped by same medication name, then ordered by descending event time

        prevMed = ''
        seq = []
        result = {}
        for e in events:
            if e['medication_name'] == prevMed or prevMed == '':
                seq.append(e)
            else:
                periodsCalc = EventManager()
                periodsCalc.processEventSequence(seq)
                result[prevMed]=periodsCalc.periods
                seq = []

            return jsonify({'periods': result})
    except:
        return jsonify({'error'})

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    rmqCon = loop.run_until_complete(initRMQ(loop))
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(rmqCon.close())
    
    