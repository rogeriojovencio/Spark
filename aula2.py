import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf



if __name__ == "__main__":

    def print_values(x):
        print(f'{x[0]}: {x[1]}')
        

    
    spark = SparkSession.builder.appName("Count Words").getOrCreate()
    sc=spark.sparkContext




    path = "/home/rogerio/pySpark/lorem_ipsum.txt"
    df1=sc.textFile(path)
    # print(df1.collect())
 
    df1=df1.map(lambda x:str(x).replace(".",""))
    df1=df1.map(lambda x:str(x).replace(",",""))
    df1=df1.map(lambda x:str(x).replace(";",""))
    df1=df1.map(lambda x:str(x).lower())
    df2=df1.flatMap(lambda x: x.split(" "))

    # df3=df2.map(print_values).collect()

    df3=df2.map(lambda x:((x,1)))
    # print(df5.collect())

    df4=df3.reduceByKey(lambda x,y : x+y)
    # print(df6.collect())

    df4=df4.filter(lambda x:x[0]!="")

    df5=df4.sortBy(lambda x:x[1])
    # print(df5.collect())
    
   # df5.map(print_values).collect()
  
    schema2 ="Valores STRING, Total INT"
    df6 = spark.createDataFrame(df5, schema2)     
    df6.show(10)

    agrupado =df6.groupBy("Valores").agg(sum("Total"))
    agrupado.show(10)

    agrupado2 = df6.select("Valores", "Total").where(pyspark.sql.functions.col("Total")>20)  
    agrupado2.show(10)

    #df6.write.format("csv").save("/home/rogerio/df5importcsv")
    #df6.write.format("json").save("/home/rogerio/df5importjson")


    spark.stop()
