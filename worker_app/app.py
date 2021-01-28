import redis
import json
import psycopg2

r = redis.Redis(host="super-redis", port=6379)
conn = psycopg2.connect("dbname='postgres' user='postgres' host='super-db' password='postgres'")
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS votes(client_id text, vote_option text)")
print("Table created ...")

r.lpush("votes", json.dumps({"client_id": "Abdul", "vote_option": "Cats"}))
r.lpush("votes", json.dumps({"client_id": "Mia", "vote_option": "Cats"}))
r.lpush("votes", json.dumps({"client_id": "Grace", "vote_option": "Dogs"}))

print('Sample votes pushed to redis ...')

item = r.blpop("votes", 0)
print(item)
vote = json.loads(item[1])
print(vote)

print('Sample votes retrieved from redis ...')

cursor.execute("INSERT INTO votes VALUES(%s, %s)", (vote["client_id"], vote["vote_option"]))

print("INSERTed ...")

conn.commit()

print("Committed ...")

print('Program ending ...')