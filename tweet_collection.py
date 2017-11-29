import tweepy
import time
import psycopg2 as dbapi

# USER = "aaroadwatch" #this account just tweets dublin collisions
USER = 'LiveDrive'
ckey = 'JynjVApSmQ39RzbTwwpJfiXVY'
csecret = 'Z8DxLRBDhMZDGTN02SQRHAuDDconQwFfTyTNYX9UyZjElnzbXO'
atoken = '519563654-eJZmS2mIREIZqPqmlpehIihP2Ne8KFQaOxmwAmYl'
asecret = 'oBdrAtiQItOuRP46M1jqOTs2dTQqV578o40mXi3BJSOHq'

# connect to twitter api
auth = tweepy.OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth, wait_on_rate_limit=True)  # connect to twitter api


def insert_prepare(status, location):
    id = status.id


def dbinsert(status, location):
    tweetid = str(status.id)
    timestamp = status.created_at

    ##check if location exists
    db_cursor.execute("SELECT count(*) FROM locations where (location = %s);", (location,))
    result = db_cursor.fetchone()[0]
    # if location does not exist
    if (result == 0):
        try:
            ##insert location
            db_cursor.execute("INSERT INTO locations(location) values(%s);", (location,))
            db_connection.commit()
        except:
            print("error in inserting location")

    # check if collision exists
    db_cursor.execute("SELECT count(*) FROM collisions where (location = %s and collisionTime = %s);",
                      (location, timestamp))
    result = db_cursor.fetchone()[0]
    # if it does not exist
    if (result == 0):
        try:
            # insert into collisions
            db_cursor.execute("INSERT INTO collisions(location, collisionTime) values(%s, %s);", (location, timestamp))
            db_connection.commit()
        except:
            print("error in inserting collision")

    # check if tweet exists
    db_cursor.execute("SELECT count(*) FROM tweets where (tweetid = %s);", (tweetid,))
    result = db_cursor.fetchone()[0]

    # if tweet does not exist
    if (result == 0):
        try:
            # get relavent collisions id
            db_cursor.execute("SELECT collisionID FROM collisions where (location = %s and collisionTime = %s);",
                              (location, timestamp))
            result = db_cursor.fetchone()[0]

            # insert tweet with relavent id
            db_cursor.execute("INSERT INTO tweets(collisionID, tweetID) values(%s, %s);", (result, tweetid))
            #   data1 = {'collisionID':1, 'tweetID':"333"}
            #   cur.execute("""INSERT INTO tweets(collisionID, tweetID) values(%(collisionID)s, %(tweedID)""", data1)
            db_connection.commit()
        except:
            print("error in tweet insert")

    return;


def aaroadwatch_filter(status):
    if (not status.in_reply_to_status_id):  # if tweet is not a reply
        try:
            if (status.entities["hashtags"][0]["text"] == "DUBLIN"):  # if hashtag is dublin
                text = status.text.lower()  # get text in lowercasee
                if ("crash" in text or "collision" in text):  # verify they are a crash or collision
                    # this is to try limit the text to a location
                    if (" at " in text):
                        text = text.split(" at ")[1]
                    elif (" on " in text):
                        text = text.split(" on ")[1]
                    else:
                        text = text.split("#dublin")[1]
                    if ("," in text):
                        text = text.split(",")[0]
                    else:
                        text = text.split(".")[0]
                    print(text)
        except TypeError:
            print()
        except IndexError:
            print()
    return;


def livedrive_filter(status):
    if (not status.in_reply_to_status_id):  # if tweet is not a reply
        text = status.text.lower()  # get text in lowercasee
        if (("crash" in text or "collision" in text) and ":" in text):  # verify they are a crash or collision
            location = text.split(":")[0]
            dbinsert(status, location)


bootrun = True;
while True:
    try:
        # connect to db
        db_connection = dbapi.connect(database='fyp', user="postgres")
        db_cursor = db_connection.cursor()

        #if time going through this loop
        if bootrun:
            db_cursor.execute("SELECT MIN(tweetID) FROM tweets;") # get the lowest value tweet in database
            pointer = db_cursor.fetchone()[0]
            bootrun = False;

        if (pointer == None): #if database is empty
            tweets = api.user_timeline(screen_name=USER, count=100, include_rts=False)  # query user for tweets

        else:
            # get all tweets from users timeline, max_id= limits use to tweets below this id aka to resume where previously
            tweets = api.user_timeline(screen_name=USER, max_id=pointer, count=100, nclude_rts=False)  # query user for tweets
        print(pointer)
        for status in tweets:
            # aaroadwatch_filter(status)
            pointer = status.id
            livedrive_filter(status)

    except tweepy.RateLimitError:
        print("except")
        db_cursor.close()
        db_connection.close()
        print("limit reached wait 15mins")
        time.sleep(15 * 60)
