from itertools import combinations
from pyspark import SparkContext

sc=SparkContext("local","BookPairs")

lines = sc.textFile("/home/cs143/data/goodreads.dat")

books=lines.map(lambda line: [int(book) for book in (line.replace(":",",").split(",")[1:])])
books=books.filter(lambda book_list: len(book_list) >=2)

sorted_books=books.map(lambda book_list:sorted(book_list))

pairs=sorted_books.flatMap(lambda book_list: combinations(book_list,2))
pairs=pairs.map(lambda pair: (pair,1))

pair_counts=pairs.reduceByKey(lambda a,b:a+b)
pair_counts=pair_counts.filter(lambda entry: entry[1]>20)

pair_counts.saveAsTextFile("/home/cs143/output1")