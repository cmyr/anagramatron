import twitterhandler
stream = twitterhandler.StreamHandler()
stream.start()
for t in stream:
    print(t.get('text'))