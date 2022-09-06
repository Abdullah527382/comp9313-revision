import json
from mrjob.job import MRJob

class MaxTemp(MRJob):

    JOBCONF = {
        'map.output.key.field.separator': '\t',
        'mapred.reduce.tasks':2, 
        'mapreduce.partition.keypartitioner.options':'-k1', 
        'partitioner':'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'
    }

    SORT_VALUES = True
    def mapper(self, _, line):
        year, temp = line.split(" ")
        yield (year, int(temp))

    def combiner(self, key, values):
        
        yield (key, max(values))

    def reducer(self, key, values):
        yield key, max(values)

if __name__ == '__main__':

    MaxTemp.run()