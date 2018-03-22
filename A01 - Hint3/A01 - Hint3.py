# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs


# ------------------------------------------
# FUNCTION my_line_parse
# ------------------------------------------
def my_line_parse(x):
  stringSplit = x.split(" ")
  res = stringSplit[0]+"\t("+stringSplit[1]+", "+stringSplit[2]+")"
  return res
# ------------------------------------------
# FUNCTION my_filter
# ------------------------------------------
def my_filter(x):
  res = False
  strSplit = x.split("\t")  
  langSplit = strSplit[0].split(".")
  for language in languages:
    if language == langSplit[0]:
      res=True 
      break
  return res
# ------------------------------------------
# FUNCTION my_groupBy
# ------------------------------------------
def my_groupBy(x):
  split = x.split("\t")
  return split[0]
# ------------------------------------------
# FUNCTION my_sort
# ------------------------------------------
def my_sort(x):
  res = [("",0),("",0),("",0),("",0),("",0)]
  for ele in x:
    split = ele.split(", ")
    pageViews =int(split[1].replace(")","",1)) 
    if  pageViews > res[4][1]:
      pageNameSplit=split[0].split("\t")
      pageName=pageNameSplit[1].replace("(","",1)
      res[4]=(pageName,pageViews)
      res.sort(key=lambda value:value[1],reverse=True)
  return res  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    # 2. Read data from dataset into a RDD
    inputRDD = sc.textFile(dataset_dir)
    # 3. Parse data in the data set in to a more readable format
    parsedRDD = inputRDD.map(my_line_parse)
    # 4. Filter the data so only the languages we want remain
    filteredRDD = parsedRDD.filter(my_filter)
    # 5. Group all of the data based on the language/project key. 
    groupedKeysRDD = filteredRDD.groupBy(my_groupBy)
    # 6. Sort all of the values in each group to only get the top 5
    sortedRDD = groupedKeysRDD.mapValues(my_sort)
    # 7. Save the resulting data into a txt file 
    sortedRDD.saveAsTextFile(o_file_dir)
  
	# Complete the Spark Job

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
