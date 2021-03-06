import psycopg2
import yaml
import textwrap
from twitter import *

with open('config.yaml') as f:
    config = yaml.load(f)

def twit():
    '''
    initiate twitter connection.
    '''
    tcf = config['twitter']
    
    auth = OAuth(
    consumer_key = tcf['consumer_key'],
    consumer_secret = tcf['consumer_secret'],
    token = tcf['token'],
    token_secret = tcf['token_secret'],
    )
    
    t = Twitter(auth=auth)
    
    return t

def db_conn():
    '''
    initiate database connection to postgres.
    '''
    pgsql = config['pgsql']

    conn = psycopg2.connect(
        dbname = pgsql['dbname'],
        user = pgsql['user'],
        password = pgsql['password'],
        host = pgsql['host'],
        )
    return conn

class Skill:
        
    def __init__(self, skill):
        sid, index, app, eg, value, description = skill
        self.skill = skill
        self.sid = sid
        self.index = index
        self.app = app
        self.eg = eg
        self.value = value
        self.description = description
        
    @property
    def tweet_text(self):
        tweet = '{0[2]} \n'\
        'EG: {0[3]} \n'\
        'Value: {0[4]} \n'\
        '{0[1]}. {0[5]}'.format(self.skill)
        return tweet

    def tweetable(self):
        if len(self.tweet_text()) < 140:
            return True
        else:
            return False
    
    @property
    def construct_tweet(self):
        if self.tweetable():
            return [self.tweet_text]
        else:
            tweet = []
            chunks = textwrap.wrap(self.tweet_text, width=130, replace_whitespace=False)
            for i, chunk in enumerate(chunks):
                tweet.append("{}/ {}".format(i+1, str(chunk)))
            return tweet

# initiate twitter and database objects
t = twit()
con = db_conn()
cur = cursor()

sql_fetch = '''
    SELECT 
        skills.skill_id,
        skills.index, 
        skills.apparatus,
        skills.eg,
        skills.value,
        skills.description
    FROM 
        public.skills
    WHERE
        skills.tweeted = FALSE
    ORDER BY
        apparatus='Horizontal Bar',
        apparatus='Parallel Bars',
        apparatus='Vault',
        apparatus='Rings',
        apparatus='Pommel Horse',
        apparatus='Floor Exercise',
        eg,
        index
    LIMIT
        1
    '''

sql_modify = '''
    UPDATE
        skills
    SET
        tweeted = TRUE
    WHERE
        skill_id = %s
'''

sql_reset = '''
    UPDATE
        skills
    SET
        tweeted = FALSE
'''

def get_skill(sql):
    try:
        cur.execute(sql)
        (skill,) = cur.fetchall()

        sid, index, app, eg, value, description = skill
    except:
        skill = False
    
    return skill

def update_skill(sql, skill):
    sql_params = (skill.sid,)

    cur.execute(sql, sql_params)
    con.commit()

def reset_skill_tweets(sql):
    cur.execute(sql)
    con.commit()


if not get_skill(sql_fetch):
    reset_skill_tweets(sql_reset)
else:
    skill = Skill(get_skill(sql_fetch))
    update_skill(sql_modify, skill)
    tweet_list = skill.construct_tweet

    for chunk in tweet_list:
        t.statuses.update(status=chunk)

con.close()

