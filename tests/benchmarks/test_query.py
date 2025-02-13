import random

CREATE_TABLE = """
CREATE TABLE customer (
        id SERIAL NOT NULL,
        name VARCHAR(255),
        description VARCHAR(255),
        q INTEGER,
        p INTEGER,
        x INTEGER,
        y INTEGER,
        z INTEGER,
        PRIMARY KEY (id)
)
"""
DROP_TABLE = "DROP TABLE IF EXISTS customer"

INSERT = """
INSERT INTO customer (id, name, description, q, p, x, y) VALUES
(%(id)s, %(name)s, %(description)s, %(q)s, %(p)s, %(x)s, %(y)s)
"""

select = """
SELECT customer.id, customer.name, customer.description, customer.q,
    customer.p, customer.x, customer.y, customer.z
FROM customer
WHERE customer.id = %(id)s
"""


def test_query(benchmark, conn):
    ids = range(100)
    data = [
        dict(
            id=i,
            name="c%d" % i,
            description="c%d" % i,
            q=i * 10,
            p=i * 20,
            x=i * 30,
            y=i * 40,
        )
        for i in ids
    ]

    cur = conn.cursor()
    cur.execute(DROP_TABLE)
    cur.execute(CREATE_TABLE)
    cur.executemany(INSERT, data)

    @benchmark
    def query():
        rand_id = random.choice(ids)
        cur.execute(select, {"id": rand_id})
        cur.fetchall()
