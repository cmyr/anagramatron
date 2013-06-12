Anagramer hunts for anagrams on Twitter.

http://anagramatron.tumblr.com
http://twitter.com/anagramatron


anagramer.Anagramer() is the core script, which uses classes in twitterhandler to connect to the twitter streaming and REST api, and classes in datahandler to archive and retrieve possible anagrams for likeness comparison.

there is also a small bottle-powered webserver that allows remote review of possible anagrams. There is a companion iPhone app that I use to check up on progress.

when hits are approved (manually) they are automatically posted to associated twitter and tumblr accounts.

- The vast majority of 'hits' are tweets that are identical.
- The vast majority of remaining hits are either tweets that have one letter switched ('I hate u' vs. 'I haet u') or that have the same words in a different order ('bitch plz' vs. 'plz bitch!!'). etc.

 
