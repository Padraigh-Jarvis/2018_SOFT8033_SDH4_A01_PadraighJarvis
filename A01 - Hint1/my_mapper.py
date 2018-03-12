#!/usr/bin/python

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
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    #Create dictionaries needed for the mapper 
    results = {}
    for language in languages:
        results[language]={}
 
    for line in input_stream:
        #Then split line by white space

        lineContents = line.split(" ")
        #if will check if the line is an right to left language 
        if lineContents[0] is 0:
            continue
        #Check if the language of the line matches one we are looking for
        for language in languages:
            languageProjectKey = lineContents[0].split(".")
            if language == languageProjectKey[0]:
                #Check if the project key exists in the dictionary.
                if lineContents[0] not in results[language]:
                    #Create new list
                    results[language][lineContents[0]] = [("",0),("",0),("",0),("",0),("",0)]
                #Add new data to list
                
                if results[language][lineContents[0]][4][1]<int(lineContents[2]):
                    results[language][lineContents[0]][4] = (lineContents[1],int(lineContents[2]))
                    results[language][lineContents[0]] = sorted(results[language][lineContents[0]], key=lambda value:value[1], reverse=True)
    
    
    #print(results["en"])
    #Begin output process   
    for language in languages:
        for projectKey in results[language]:
            #Sort the list based on the value (ie [1]) of the tuple in accending order
            
            #sortedResults = sorted(results[language][projectKey] , key=lambda value:value[1] , reverse=True)
            #Range used for getting the top n results
            for index in range(0,num_top_entries):
                #If to make sure the project has enough items.
                if len(results[language][projectKey])>index and results[language][projectKey][index][1]>0:
                    output = projectKey+"\t("+results[language][projectKey][index][0]+","+str(results[language][projectKey][index][1])+")\n"
                    output_stream.write(output)
        
            
    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "../../my_dataset/pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
