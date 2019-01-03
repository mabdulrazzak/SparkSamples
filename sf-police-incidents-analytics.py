##############################################################
#
# Purpose: This prgram uses the Kaggle dataset and uses Apache Spark
# to create stats and analytics on the data. 
# Program is divided into parts
############################################################# 

from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields = line.split(',')
    category = fields[1]
    desc = fields[2]
    dayOfWeek = fields[3]
    pdDistrict = fields[6]
    address = fields [8]
    return (category, desc, dayOfWeek, pdDistrict, address)

conf = SparkConf().setMaster("local").setAppName("CrimesHistogram")
sc = SparkContext(conf = conf)

# NOTE: Make sure the file path below is where the police-department-incidents.csv is stored during the execution of this code. 
# Data obtained from https://www.kaggle.com/san-francisco/sf-police-calls-for-service-and-incidents/version/151
lines = sc.textFile("file:///police-department-incidents.csv")
incidents = lines.map(parseLine)

###############################################################################
## Part 1: Count the incident reports per category and print them in the ascending order
################################################################################

#get the total records count
totalRecords = incidents.count()

#get count per crime category
categoryRdd = incidents.map(lambda x:x[0])
crimeByCategory = categoryRdd.countByValue() #returns a dictionary of key (i.e. category) and count of that category)


# print total crimes and count per category
#sorting by crime count - Done via the second param sorted (dictionary, key on which to sort (here the second attribute i.e. value is specified)
sortedResults = collections.OrderedDict( sorted (crimeByCategory.items(), key=lambda x:x[1] ))

for key, value in sortedResults.items():
    print("Category = %s count = %i" % (key, value))
    
print ("Total number of crimes reported are %i" % totalRecords)

# Next: For each category Find the top crime (by countByValue on desc and finding the max of the desc)
