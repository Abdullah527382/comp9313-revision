from mrjob.job import MRJob
import re

class CoTermNSPair(MRJob):

    def mapper(self, _, line):
        words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
        words = list(filter(None, words))

        for i, word in enumerate(words):
            wordList = words[i+1:len(words)]
            # Generate a pair of (w, u) where u appears after w in words
            pairList = self.pairGen(word,wordList)
            for pair in pairList:
                yield(pair, 1) 

    def reducer(self, pair, counts):
        yield(pair, sum(counts))

    # Given a word and a list of words, return a list of pairs
    def pairGen(self, w, words):
        pairList = []
        for u in words:
            pair = w + " " + u
            pairList.append(pair)
        return pairList

if __name__ == '__main__':

    CoTermNSPair.run()
