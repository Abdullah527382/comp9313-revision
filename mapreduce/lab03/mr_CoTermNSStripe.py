from mrjob.job import MRJob
import re
from collections import Counter

class CoTermNSPair(MRJob):

    def mapper(self, _, line):
        words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
        words = list(filter(None, words))

        for i, word in enumerate(words):
            wordList = words[i+1:len(words)]
            wordStripe = {}
            for u in wordList:
                # Check if word exists in dict
                if (wordStripe.get(u)):
                    wordStripe[u] += 1
                else:
                    wordStripe[u] = 1
            yield(word, wordStripe)

    def reducer(self, word, stripes):
        finalStripe = {}
        for stripe in stripes: 
            for key in stripe:
                # If key exists in our overall stripe
                if (finalStripe.get(key)):
                    finalStripe[key] += stripe[key]
                else:
                    # If key doesn't exist
                    finalStripe[key] = stripe[key]
        # Loop through our overall stripe and output:
        for key in finalStripe:
            yield(word +" " + key, finalStripe[key])
        
if __name__ == '__main__':

    CoTermNSPair.run()
