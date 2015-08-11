![Anagramatron](http://www.cmyr.net/wptest/wp-content/uploads/2013/05/Untitled-1.png)
## Anagramatron hunts for anagrams on Twitter.


http://anagramatron.tumblr.com

http://twitter.com/anagramatron

#### What
Anagramatron hunts for pairs of tweets that use the exact same set of letters. 

This script connects to the twitter stream. When it receives a new tweet it runs it through some filters, ignoring tweets that contain things like links or @mentions, or that contain less then a minimum number of characters.

For each tweet that passes these filters, the occurrences of each letter are counted; this serves as a simple anagram-unique hash, i.e. any pair of anagrams sorted this way will produce identical strings.

This hash is checked against a list of all the hashes we have stored so far. If nothing is found, the hash and the original text are saved in a database. If a match is found, the original text of both tweets are run through some comparison tests to check for like-ness. If they pass that text they are flagged for review, to make sure they aren't too similar, or haven't been posted previously, etcetera.

#### How
anagramatron.py contains the main run loop, which uses classes in twitterhandler to connect to the twitter streaming and REST api, and classes in anagramfinder to archive and retrieve possible anagrams for likeness comparison. Data storage is handled by multidbm.py, a wrapper for a flexible number of dbm databases, which lets older tweets be automatically removed when the database gets too full.

there is also a small bottle-powered webserver that allows remote review of possible anagrams. There is a companion iPhone app that I use to check up on progress.

when hits are approved (manually) they are automatically posted to associated twitter and tumblr accounts.

- The vast majority of 'hits' are tweets that are identical.
- The vast majority of remaining hits are either tweets that have one letter switched ('I hate u' vs. 'I haet u') or that have the same words in a different order ('hi twitter!!' vs. 'twitter, hi?'). etc.

#### FAQ

*Other questions and comments can be directed to [@cmyr](http://www.twitter.com/cmyr) or [anagrams@cmyr.net](mailto:anagrams@cmyr.net)*


Q: Is this manually curated?

A: Mostly for issues of volume ( there are a lot of variations of 'goooood mooornnniinng!', there are a lot of spam bots posting subtely different versions of the same message, etc) the bot doesn't automatically post every anagram it finds. Essentially there's an [iphone client](https://github.com/cmyr/anagram-review-ios) that reviews matches, which are manually approved or rejected. 

Q: How does this handle numerals / non-latin characters? 

A: Most tweets that contain non-latin characters are ignored. Punctuation and emoji are not counted when considering anagramdom. 

Q: What is the relationship between the twitter page and the tumblr?

A: One-to-one. When a match is approved, it gets posted to both. 

#### Dependencies:
this script makes use of [python twitter tools](http://mike.verdone.ca/twitter/) for handling twitter interactions, [tumblpy](https://github.com/michaelhelmick/python-tumblpy) for posting to tumblr, and [bottle](http://bottlepy.org/docs/dev/) + [cherrypy](http://www.cherrypy.org/) to run a webserver.

 
