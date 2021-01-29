import redis
import json
import sys
import time
import logging
import psycopg2

# Set this to level=loging.INFO to see all the debugging issues.
logging.basicConfig(level=logging.WARNING)

DB_SERVER_INFO = "dbname='postgres' user='postgres' host='super-db' password='postgres'"
RETRY_LIMIT = 10

# Exponential ... (0.4*exp(1:10))
RETRY_INTERVALS = [1.5, 2.2, 3.3, 5.0, 7.4, 11.0, 16.4, 24.5, 36.6, 54.6]

cursor = None
item = None
vote = None

def refreshRedisConnection():
  r = None
  retries = 0
  while r == None and retries < RETRY_LIMIT:
    logging.info('Connecting to super-redis ...')
    try:
      r = redis.Redis(host="super-redis", port=6379)
      logging.info('Connected! (super-redis)')
      return r
    except:
      r = None

      # Wait for RETRY_INTERVALS[retries] seconds before retrying connection.
      logging.warn("Retrying super-redis connection ...")
      time.sleep(RETRY_INTERVALS[retries])
      retries += 1

  if retries >= RETRY_LIMIT:
    logging.warn('Redis server (super-redis) timed out. Worker shutting down ...')
    sys.exit(1)

def refreshDBConnection():
  conn = None
  retries = 0
  while conn == None and retries < RETRY_LIMIT:
    logging.info('Connecting to super-db ...')
    try:
      conn = psycopg2.connect(DB_SERVER_INFO)
      logging.info('Connected! (super-db)')
      return conn
    except:
      conn = None

      # Wait for RETRY_INTERVALS[retries] seconds before retrying connection.
      logging.warn("Retrying super-db connection ...")
      time.sleep(RETRY_INTERVALS[retries])
      retries += 1

  if retries >= RETRY_LIMIT:
    logging.warn('Db server (super-db) timed out. Worker shutting down ...')
    sys.exit(1)


r = refreshRedisConnection()
conn = refreshDBConnection()


try:
  cursor = conn.cursor()
  # cursor.execute("DROP TABLE votes")
  cursor.execute("CREATE TABLE IF NOT EXISTS votes(client_id TEXT PRIMARY KEY, vote_option TEXT)")
  logging.info("Table re/initialized ...")
except:
  conn = refreshDBConnection()


######### TEST DATA ENTRY (DEBUGGING PURPOSES) ###########

# test_data_pushed = False

# while not test_data_pushed:
#   try: 

#     # Clear list to solve UNIQUE-CONSTRAINT issues later.
#     r.delete("votes")

#     r.lpush("votes", json.dumps({"client_id": "Abdul", "vote_option": "Cats"}))
#     r.lpush("votes", json.dumps({"client_id": "Mia", "vote_option": "Cats"}))
#     r.lpush("votes", json.dumps({"client_id": "Grace", "vote_option": "Dogs"}))
#     r.lpush("votes", json.dumps({"client_id": "Grace", "vote_option": "Cats"}))
#     r.lpush("votes", json.dumps({"client_id": "Grace", "vote_option": "Dogs"}))
    
#     test_data_pushed = True
#     logging.info('Sample votes pushed to redis ...')
#   except Exception as e:
#     logging.info('Exception (test_data_pushed): {}'.format(type(e)))
#     r = refreshRedisConnection()

#####################################


# This is the main worker loop.
# We pop items from redis. We INSERT them into our db.
while True:
  logging.info('REACHED main-while loop!')
  while item == None:
    # Keep this worker running by calling redis every 4 seconds.
    #   r.blpop returns None when timeout ends and there was
    #   no queued item to consume.
    try:
      item = r.blpop("votes", timeout=4)
      logging.info('Finished blpop!')
    except Exception as e:
      # Maybe redis went down.
      logging.info('Except blpop!: {}'.format(e))
      r = refreshRedisConnection()
    
  vote = json.loads(item[1])

  # Set item to None for the next blpop loop.
  item = None

  try:
    cursor.execute("INSERT INTO votes VALUES(%s, %s)", (vote["client_id"], vote["vote_option"]))
    logging.info("Inserted {}!".format(vote))
    conn.commit()
    logging.info("Committed!")

    # Might require a 'while not committed: ... keep trying to commit?'
  except psycopg2.errors.UniqueViolation:
    logging.info("Should see this twice (if popping from sample redis insertions)!")

    # Reason for rollback: https://stackoverflow.com/a/31146267
    conn.rollback()
    cursor.execute("UPDATE votes SET vote_option = %s WHERE client_id = %s", (vote["vote_option"], vote["client_id"]))
    logging.info("UPDATED!")
    conn.commit()
  except:
    # Maybe db went down.
    conn = refreshDBConnection()
    cursor = conn.cursor()