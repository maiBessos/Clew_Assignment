# aio_pika template shamelessly stolen from: https://aio-pika.readthedocs.io/en/latest/quick-start.html

import json
import asyncio
import json
import os
import datetime
import traceback # for printing exceptions
import iso8601 # datetime.fromiso() failed parsing the json examples. iso8601 lib to the rescue!

import aiopg # async postgres wrapper

from aio_pika import connect_robust, IncomingMessage
#from flask import Flask, jsonify, request
from quart import Quart, jsonify, request # similar to Flask, but builds on asyncio loop

# PG connection constants:
PG_HOST = 'localhost' # wasn't sure how to use the local PGDATA env. varaible, so assuming localhost
def getPostgresDBName():
    envVar = os.environ.get('POSTGRES_DB')
    if envVar is None:
        return 'clew_medical'
    return envVar
def getPostgresDBUsername():
    envVar = os.environ.get('POSTGRES_USER')
    if envVar is None:
        return 'admin'
    return envVar
def getPostgresDBPass():
    envVar = os.environ.get('POSTGRES_PASSWORD')
    if envVar is None:
        return 'password'
    return envVar
dbconstr='dbname=' + getPostgresDBName() + ' user=' + getPostgresDBUsername() + ' password=' + getPostgresDBPass() + ' host=' + PG_HOST 

tablename='raw_events'
maxMedNameLen = 255

# pgpool will be set with DB connection init, and released when app exits
pgpool=None


class EventManager: # to improve: move class to seaprate file
    """ utility for creating start-stop periods from a sequence of actions.
    see processEventSequence() for assumptions and rules for period creation.
    """
    def process_start(self, e):
        if self.startsToCancel > 0:
            self.startsToCancel -= 1
        elif len(self.stops) > 0: # process the current start only if there's a future stop
            # this start event was not cancelled in the future, and there is a matching (uncanceled) stop event.
            # append a period with start time (specified in e), and end time (latest time added to self.stops)
            self.periods.append(("from:" + e['event_time'].isoformat(), "to:" + self.stops.pop().isoformat()))
            self.stops = [] # ignore non-earliest stops
            # NOTE: we may want to fix the just-added period, if we find an earlier start time (without an earlier stop)
        elif len(self.stops) == 0 and len(self.periods) > 0: # the same period has several overlapping starts
            self.periods[-1]= ("from:"+e['event_time'].isoformat(), self.periods[-1][1]) # update the previously-added period, to an earlier start time
    def process_stop(self, e):
        if self.stopsToCancel > 0:
            self.stopsToCancel -= 1
        else:
            self.stops.append(e['event_time'])
    def process_cancel_start(self, e):
        self.startsToCancel=self.startsToCancel+1
        return
    def process_cancel_stop(self, e):
        self.stopsToCancel=self.stopsToCancel+1
        return
    
    # action_list determines order of events, if events have the same event time (action_list[0] occurs before action_list[1] etc.)
    action_list = ['start', 'cancel_start', 'stop', 'cancel_stop'] 
    action_operation=[process_start, process_cancel_start,  process_stop, process_cancel_stop]
    def __init__(self):
        self.periods = [] # will include tuples: (start datetime, end datetime)
        self.startsToCancel=0
        self.stopsToCancel=0
        self.stops=[]
    def processEventSequence(self, sequence):
        """
        sequence is an event list, sorted by descending event time. 
        All events refer to the same medication.

        implementation assumptions:    
        1) cancel requests may be given in any order, and each cancel_start(cancel_stop) will cancel the latest start(stop)
        that came before it, even if there were stop(start) and/or cancel_stop(cancel_start) events between them.
        2) if there was an unpaired event (could be any of start, stop, cancel_start, cancel_stop), ignore it
        3) the event time in cancel_stop and cancel_start doesn't indicate the specific event to cancel, namely the
        time should be ignored
        4) if the sequence had multiple start actions, and then multiple stop actions, we merge the actions into one period, such that only the earliest stop and the earliest start are used
        """
        for e in sequence:
            self.action_operation[int(e['actionnum'])](self,e)

async def process_message(message: IncomingMessage):
    """ consume rabbitMQ messages
    """
    async with message.process():
        ev = json.loads(message.body)
        
        try:
            uid = int(ev['p_id'])
            ttime = iso8601.parse_date(ev['event_time'])
            medname = ev['medication_name']
            
            # index in action_list determines order of events, if events have the same event time (action_list[0] occurs before action_list[1] etc.).
            # In particular, we prioritize cancels, i.e. if action and canel-action are received at once, then the action will indeed be cancelled
            # In addition, we allow periods zero-length periods, so start and stop at the same time means period of 0
            actionPriority = EventManager.action_list.index(ev['action']) # throws exception if not a legal action
            
            if uid < 0 or len(medname) >= maxMedNameLen or len(medname) == 0:
                return # dont requeue message
            
        except: # event is missing a field or mismatching format. no point re-queueing the message:
            return
        
        
        sql_query = "INSERT INTO " + tablename + " VALUES (%s,%s,%s,%s)"
        # before executing, sanitize user_input (e.g. to avoid SQL injections). 
        # stored-procedures/psycopg2.sql string-composition are still better, but I assume its outside scope of this exercise
        global pgpool
        #timestr = ttime.strftime('%Y-%m-%d %H:%M:%S')
        timestr = ttime.isoformat()
        async with pgpool.acquire() as con:
            async with con.cursor() as cur:
                await cur.execute(sql_query, (str(uid), timestr,str(actionPriority),medname))
    
     
async def initRMQ(loop):
    # connect_robust also auto-recovers from connection problems (as opposed to connect() )
    connection = await connect_robust(host=os.environ.get('RABBIT_HOST'),
                               login=os.environ.get('RABBIT_USER'),
                               password=os.environ.get('RABBIT_PASS'),
                               loop=loop
                               )
    # Creating channel
    channel = await connection.channel()

    # process at most 4 events simultaneously
    await channel.set_qos(prefetch_count=4)

    # Declaring queue
    queue = await channel.declare_queue('events', auto_delete=True)

    await queue.consume(process_message, no_ack=False) # if process fails, we want it to get re-queued

    return connection


# async def prepDBTable(conn):
    
#     await conn.cursor().execute("DROP TABLE IF EXISTS " + tablename)
#     # TO IMPROVE: move query below to separate file/stored procedure
#     await conn.cursor().execute("CREATE TABLE " + tablename + " (p_id int, event_time TIME, action varchar(13), medication_name varchar(" + str(maxMedNameLen) + "), CONSTRAINT all_pk PRIMARY KEY (p_id,event_time,action,medication_name))")            
    

async def main(loop):
    global pgpool
    pgpool = await aiopg.create_pool(dbconstr)
    pgcon = await pgpool.acquire()
    
    async with pgcon.cursor() as cur:
        # creates/overrides table for storing events:
        # (TO IMPROVE: move query below to separate file/stored procedure)
        await cur.execute("DROP TABLE IF EXISTS " + tablename)
        await cur.execute("CREATE TABLE " + tablename + " (p_id int, event_time TIMESTAMP, actionnum int, medication_name varchar(" + str(maxMedNameLen) + "))") #, CONSTRAINT all_pk PRIMARY KEY (p_id,event_time,action,medication_name)
    

quartApp = Quart(__name__)

async def getPeriods(pid):
    eventTuples = []
    global pgpool
    # numeric value of row['actionnum'] coresponds action_list, and determines order of events, if events have the same event time (action_list[0] occurs before action_list[1] etc.).
    # In particular, we prioritize cancels, i.e. if action and canel-action are received at once, then the action will indeed be cancelled
    # In addition, we allow periods zero-length periods, so start and stop at the same time means period of 0
    async with pgpool.acquire() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT p_id, event_time, actionnum, medication_name FROM " + tablename + " WHERE p_id=%s  ORDER BY medication_name, event_time DESC, actionnum DESC", (str(pid),))
            eventTuples = await cur.fetchall()
            
    
    events = []
    for et in eventTuples:
        events.append({'p_id':et[0],'event_time':et[1], 'actionnum':et[2], 'medication_name':et[3]})
    # events will list all raw events for the user. 
    # we get results grouped by same medication name, then ordered by descending event time, and for equal event_time we sort actions according to action_list

    prevMed = ''
    seq = []
    result = {}
    for e in events:
        if e['medication_name'] != prevMed and prevMed != '':
            periodsCalc = EventManager()
            # results in seq are: 
            # 1)grouped by same medication name, 
            # 2)ordered by descending event time,  
            # 3)for equal event_time,  actions are ordered according to action_list:
            periodsCalc.processEventSequence(seq)
            if len(periodsCalc.periods) > 0: # ignore sequences with 0 periods
                result[prevMed]=periodsCalc.periods
            seq = []

        seq.append(e)
        prevMed = e['medication_name']
    
    if prevMed != '': # make sure we process the final med:
        periodsCalc = EventManager()
        periodsCalc.processEventSequence(seq)
        if len(periodsCalc.periods) > 0: # ignore sequences with 0 periods
            result[prevMed]=periodsCalc.periods
    return result


@quartApp.route('/periods', methods=['GET'],)
async def get_tasks():
    """
    interface will provide medication periods for: p_id given in GET.
    to run tests: use any non numeric value for p_id (after rabbitMQ requests have been sent)
    """

    try:
        pid=int(request.args.get('p_id'))
        if pid < 0:
            raise Exception()
    except: # if p_id is not an int  >=0 , run tests instead
        testRes = await runTests()
        return jsonify({"Test Results": testRes})
    try:
        
        return jsonify({"periods" : await getPeriods(pid)})
    except Exception:
        return jsonify({"error":"error"})

async def runTests():
    """ I was too lazy to check the exact details are correct :( e.g. period lengths )
    """
    try:
        p = await getPeriods(1)
        if len(p["X"]) != 1:
            raise Exception("p_id=1 failed")

        p = await getPeriods(2)
        if len(p) != 0:
            raise Exception("p_id=2 failed")

        p = await getPeriods(3)
        if len(p["Z"]) != 1 or len(p["X"]) != 1:
            raise Exception("p_id=3 failed")

        p = await getPeriods(4)
        if len(p) != 0:
            raise Exception("p_id=4 failed")

        p = await getPeriods(5)
        if len(p["Z"]) != 1:
            raise Exception("p_id=5 failed")
        
        p = await getPeriods(6)
        if len(p["Z"]) != 1:
            raise Exception("p_id=6 failed")

        p = await getPeriods(7)
        if len(p) != 0:
            raise Exception("p_id=7 failed")

        p = await getPeriods(8)
        if len(p) != 0:
            raise Exception("p_id=8 failed")

        p = await getPeriods(9)
        if len(p["Z"]) != 1 or len(p["X"]) != 1:
            raise Exception("p_id=9 failed")
        
        
        p = await getPeriods(11)
        if len(p["Z"]) != 1:
            raise Exception("p_id=11 failed")
        
        p = await getPeriods(10)
        if len(p["Z"]) != 1:
            raise Exception("p_id=10 failed")

        p = await getPeriods(12)
        if len(p["Z"]) != 1:
            raise Exception("p_id=12 failed")

        p = await getPeriods(13)
        if len(p["Z"]) != 1:
            raise Exception("p_id=13 failed")
    except Exception as e:
        return str(e)
    return "All tests successfull"


if __name__ == "__main__":
    print("use 'localhost:5000//periods?p_id=x' to see periods of patient x>=0")
    print("use 'localhost:5000//periods?p_id=tests' to activate tests (after rabbitMQ messages were sent")

    lp = asyncio.get_event_loop()
    lp.run_until_complete(main(lp))
    rmqCon = lp.run_until_complete(initRMQ(lp))
    
    # To run tests, use GET with anything but a legal PID
    #asyncio.sleep(1)
    #lp.run_until_complete(runTests())

    try:
        quartApp.run(debug=True, loop=lp) # quart internally uses lp 
    finally:
        try:
            lp.run_until_complete(rmqCon.close())
        except:
            pass
        if pgpool is None:
            pgpool.close()
    
    