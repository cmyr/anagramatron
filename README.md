![Anagramatron](http://www.cmyr.net/wptest/wp-content/uploads/2013/05/Untitled-1.png)
## Anagramer hunts for anagrams on Twitter.


http://anagramatron.tumblr.com

http://twitter.com/anagramatron

#### What
This script connects to the twitter stream. When it receives a new tweet it runs it through some filters, ignoring tweets that contain things like links or @mentions, or that contain less then a minimum number of characters.

It then sorts the characters in the text in alphabetical order; this ordering serves as an anagram-unique hash, i.e. any pair of anagrams sorted this way will produce identical strings.

This hash is checked against a list of all the hashes we have stored so far. If nothing is found, the hash and the original text are saved in a database. If a match is found, the original text of both tweets are run through some comparison tests to check for like-ness. If they pass that text they are flagged as a 'hit', which can then be reviewed. 

#### How
anagramer.Anagramer() is the core script, which uses classes in twitterhandler to connect to the twitter streaming and REST api, and classes in datahandler to archive and retrieve possible anagrams for likeness comparison.

there is also a small bottle-powered webserver that allows remote review of possible anagrams. There is a companion iPhone app that I use to check up on progress.

when hits are approved (manually) they are automatically posted to associated twitter and tumblr accounts.

- The vast majority of 'hits' are tweets that are identical.
- The vast majority of remaining hits are either tweets that have one letter switched ('I hate u' vs. 'I haet u') or that have the same words in a different order ('hi twitter!!' vs. 'twitter, hi?'). etc.


#### Dependencies:
this script makes use of [python twitter tools](http://mike.verdone.ca/twitter/) for handling twitter interactions, [tumblpy](https://github.com/michaelhelmick/python-tumblpy) for posting to tumblr, and [bottle](http://bottlepy.org/docs/dev/) + [cherrypy](http://www.cherrypy.org/) to run a webserver.

 
