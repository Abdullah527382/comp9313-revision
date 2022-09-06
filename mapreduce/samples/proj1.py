import math
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class InvertedIndex(MRJob):
                
    def mapper(self, _, line):
        date, texts = line.split(",")
        if len(texts) == 0:
            return
        words = texts.split(" ")
        year = date[0:4]
        output = {}

        for word in words:
            output[word] = output.get(word, 0) + 1
        
        for word, count in output.items():
            yield f'{word}#-1', year
            yield f'{word}#{year}', count

    def combiner(self, key, values):
        word, year = key.split("#")        
        if year == "-1":
            years = set()
            for y in values:
                years.add(y)
            for y in years:
                yield f'{word}#-1', y
        else:
            localtf = sum(values)
            yield f'{word}#{year}', localtf
            
    def reducer_init(self):
        self.YF=-1	#record current word in how many years
        self.res = ""
        self.cur_word = ""
            
    def reducer(self, key, values):
        N = int(jobconf_from_env('myjob.settings.years'))
        #N = 3
        word, year = key.split("#")

        if year == "-1":
            years = set()
            for y in values:
                years.add(y)
            self.YF = len(years)            
            if self.cur_word!="":
                yield self.cur_word, self.res[:-1]

            self.cur_word = word
            self.res = ""
        else:
            tf = sum(values)
            tfidf = tf * math.log10(N/self.YF)
            self.res += year+","+ str(tfidf)+";"      #do not forget str()!      

    
    def reducer_final(self):
        yield self.cur_word, self.res[:-1]	#deal with the last one        
    
    SORT_VALUES = True

    JOBCONF = {
      'map.output.key.field.separator': '#',
      #'mapreduce.job.reduces':2,
      'mapreduce.partition.keypartitioner.options':'-k1,1',
      'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2n'
    }

    def steps(self):
        return [MRStep(reducer_init=self.reducer_init, mapper=self.mapper, reducer=self.reducer, combiner = self.combiner, reducer_final=self.reducer_final)]
           
if __name__ == '__main__':
    InvertedIndex.run()
