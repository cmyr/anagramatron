import re


def format_seconds(seconds):
    """
    convert a number of seconds into a custom string representation
    """
    d, seconds = divmod(seconds, (60*60*24))
    h, seconds = divmod(seconds, (60*60))
    m, seconds = divmod(seconds, 60)
    time_string = ("%im %0.2fs" % (m, seconds))
    if h or d:
        time_string = "%ih %s" % (h, time_string)
    if d:
        time_string = "%id %s" % (d, time_string)
    return time_string


def show_anagram(one, two):
    print one
    print two
    print stripped_string(one, spaces=True)
    print stripped_string(two, spaces=True)
    print stripped_string(one)
    print stripped_string(two)
    print ''.join(sorted(stripped_string(two), key=str.lower))


def stripped_string(text, spaces=False):
    """
    returns lower case string with all non alpha chars removed
    """
    if spaces:
        return re.sub(r'[^a-zA-Z]', ' ', text).lower()
    return re.sub(r'[^a-zA-Z]', '', text).lower()

def convert_database_formats():
    SOURCE_DB = 'data/tweetcache.db'
    DEST_DB = 'data/tweets.db'
    HITS_DB = 'data/hits.db'

    import sqlite3 as lite
    source = lite.connect(SOURCE_DB)
    cursor = source.cursor()
    cursor.execute("SELECT * FROM tweets")
    tweets = cursor.fetchall()
    cursor.execute("SELECT * FROM hits")
    hits = cursor.fetchall()
    source.close()

    new_tweet_db = lite.connect(DEST_DB)
    cursor = new_tweet_db.cursor()
    cursor.execute("CREATE TABLE tweets(id integer, hash text, text text)")
    cursor.executemany("INSERT INTO tweets VALUES (?, ?, ?)", tweets)
    new_tweet_db.commit()
    new_tweet_db.close()

    hitsdb = lite.connect(HITS_DB)
    cursor = hitsdb.cursor()
    cursor.execute("""CREATE TABLE hits
                (hit_id integer, hit_status text, one_id integer, two_id integer, one_text text, two_text text)""")
    cursor.executemany("INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?)", tweets)
    hitsdb.commit()


    # load source; copy tweets/hits to new db;
if __name__ == "__main__":
    pass