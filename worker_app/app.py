import redis
from dataclasses import dataclass
import json
from sqlalchemy import create_engine, Table, Column, String, MetaData

r = redis.Redis(host="super-redis", port=6379)
p = create_engine("postgresql+pg8000://postgres:postgres@super-db:5432/votes_db")

print(r)
print(p)

meta = MetaData(p)
print('Metadata created ...')
votes_table = Table('votes', meta, Column('client_id', String), Column('vote_option', String))
print('votes_table created ...')

r.lpush("votes", json.dumps({"client_id": "Abdul", "vote_option": "Cats"}))
r.lpush("votes", json.dumps({"client_id": "Mia", "vote_option": "Cats"}))
r.lpush("votes", json.dumps({"client_id": "Grace", "vote_option": "Dogs"}))

print('Sample votes pushed to redis ...')

with p.connect() as conn:
    print("Connection established!")
    i = 1
    while i < 10:
        item = r.blpop(0)
        print("{} popped ...".format(item))

        vote = json.loads(item)
        print("Converted fron JSON to {}".format(vote))

        conn.execute(votes_table.insert().values(client_id=vote['client_id'], vote_option=vote['vote_option']))
        print("Successfully INSERTed into DB ...")

        i += 1

print('Program ending ...')