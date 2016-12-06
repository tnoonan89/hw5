
# coding: utf-8

# Using SQLAlchemy to Talk to a Database
# =====================
# SqlAlchemy helps you use a database to store and retrieve information from python.  It abstracts the specific storage engine from te way you use it - so it doesn't care if you end up using MySQL, SQLite, or whatever else. In addition, you can use core and the object-relational mapper (ORM) to avoid writing any SQL at all.  The [SQLAlchemy homepage](http://www.sqlalchemy.org/) has lots of good examples and full documentation.

# In[ ]:

from sqlalchemy import *
import datetime
import mediacloud, datetime


# ## Basic SQL Generation
# The core library generates SQL for you.  Read more about it on their [expression language tutorial page](http://docs.sqlalchemy.org/en/rel_1_0/core/index.html). Below are some basic examples.

# ### Creating a Table
# Read more about [defining and creating tables](http://docs.sqlalchemy.org/en/rel_1_0/core/tutorial.html#define-and-create-tables).

# In[ ]:

# add `echo=True` to see log statements of all the SQL that is generated
engine = create_engine('sqlite:///:memory:',echo=True) # just save the db in memory for now (ie. not on disk)
metadata = MetaData()
# define a table to use
queries = Table('queries', metadata,
    Column('id', Integer, primary_key=True),
    Column('keywords', String(400), nullable=False),
    Column('sentCount', Integer, nullable=False),
    Column('timestamp', DateTime, default=datetime.datetime.now),
)
metadata.create_all(engine) # and create the tables in the database

mc = mediacloud.api.MediaCloud('3f5b829edb25edd536c6b109041fcc34e19d40b9d61de55f7e0f0decd904d98f')


restrump = mc.sentenceCount('trump', solr_filter=[mc.publish_date_query( datetime.date( 2016, 9, 1), datetime.date( 2016, 10, 1) ), 'tags_id_media:1' ])
resclinton = mc.sentenceCount('clinton', solr_filter=[mc.publish_date_query( datetime.date( 2016, 9, 1), datetime.date( 2016, 10, 1) ), 'tags_id_media:1' ])

print restrump['count'] # prints the number of sentences found
print resclinton['count'] # prints the number of sentences found
# ### Inserting Data
# Read more about generating [SQL insert statements](http://docs.sqlalchemy.org/en/rel_1_0/core/tutorial.html#insert-expressions).

# In[ ]:

insert_stmt = queries.insert()
str(insert_stmt) # see an example of what this will do


# In[ ]:

db_conn = engine.connect()
insert_stmt = queries.insert().values(keywords='Trump', sentCount=restrump['count'])
result = db_conn.execute(insert_stmt)
result.inserted_primary_key # print out the primary key it was assigned


# In[ ]:
db_conn = engine.connect()
insert_stmt = queries.insert().values(keywords='Clinton', sentCount=4)
result = db_conn.execute(insert_stmt)
result.inserted_primary_key # print out the primary key it was assigned


# In[ ]:


#result = db_conn.execute(insert_stmt)
#result.inserted_primary_key # print out the primary key it was assigned







# 

# 

# # Retrieving Data
# Read more about using [SQL select statments](http://docs.sqlalchemy.org/en/rel_1_0/core/tutorial.html#selecting).

# In[1]:

from sqlalchemy.sql import select
select_stmt = select([queries])
results = db_conn.execute(select_stmt)
for row in results:
    print row


# In[ ]:

select_stmt = select([queries]).where(queries.c.id==1)
for row in db_conn.execute(select_stmt):
    print row


# In[ ]:

select_stmt = select([queries]).where(queries.c.keywords.like('p%'))
for row in db_conn.execute(select_stmt):
    print row


# ## ORM
# You can use their ORM library to handle the translation into full-fledged python objects.  This can help you build the Model for you [MVC](https://en.wikipedia.org/wiki/Model–view–controller) solution.

# In[ ]:

import datetime
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()


# ### Creating a class mapping
# Read more about [creating a mapping](http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#declare-a-mapping).

# In[ ]:

class Query(Base):
    __tablename__ = 'queries'
    id = Column(Integer, primary_key=True)
    keywords = Column(String(400))
    senCount = Column(Integer)
    timestamp = Column(DateTime,default=datetime.datetime.now)
    def __repr__(self):
        return "<Query(keywords='%s')>" % (self.keywords)
Query.__table__


# ### Creating a connection and session
# Read more about [creating this stuff](http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#creating-a-session).

# In[ ]:

engine = create_engine('sqlite:///:memory:') # just save the db in memory for now (ie. not on disk)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
my_session = Session()


# ### Inserting Data
# Read more about [inserting data with an ORM](http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#adding-new-objects).

restrump = mc.sentenceCount('trump', solr_filter=[mc.publish_date_query( datetime.date( 2016, 9, 1), datetime.date( 2016, 10, 1) ), 'tags_id_media:1' ])
resclinton = mc.sentenceCount('clinton', solr_filter=[mc.publish_date_query( datetime.date( 2016, 9, 1), datetime.date( 2016, 10, 1) ), 'tags_id_media:1' ])

print restrump['count'] # prints the number of sentences found
print resclinton['count'] # prints the number of sentences found


# In[ ]:

query = Query(keywords="Trump", senCount=restrump['count'])
query.keywords


# In[ ]:

my_session.add(query)
my_session.commit()
query.id




query = Query(keywords="Clinton", senCount=resclinton['count'])
query.keywords


# In[ ]:

my_session.add(query)
my_session.commit()
query.id


# ### Retrieving Data
# Read more about [retrieving data from the db](http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying) via an ORM class.

# In[ ]:
totalCount = 0
for q in my_session.query(Query).order_by(Query.timestamp):
    print query.keywords, query.senCount

print totalCount
# In[ ]:

query1 = Query(keywords="robot")
query2 = Query(keywords="puppy")
my_session.add_all([query1,query2])
my_session.commit()


# In[ ]:

for q in my_session.query(Query).order_by(Query.timestamp):
    print q


# In[ ]:

for q in my_session.query(Query).filter(Query.keywords.like('r%')):
    print q


# In[ ]:



