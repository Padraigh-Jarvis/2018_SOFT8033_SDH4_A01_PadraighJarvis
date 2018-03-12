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
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
     #Read data from file and split them by new line char
   
    
    
    
    results = {}
    for line in input_stream.readlines():
        #Split the line into 2. First half containing the language/project
        #The second half containing the tuple
        lineContents = line.split("\t")
        
        #If a language/project does not exist in the results dictionary then add it        
        if lineContents[0] not in results:
            results[lineContents[0]] = [("",0),("",0),("",0),("",0),("",0)]
        
    
        #Read the tuple data in from the file
        #Because the tuple is read in as a string it requires some parseing 
        tupleInfo = lineContents[1].split(",")
        tupleLen = len(tupleInfo)
        
        #If only triggers if there is a ',' in the page name
        if tupleLen>2:
            tupleInfo[1] = tupleInfo[tupleLen-1]    
            tupleInfo[0] = str(tupleInfo[:tupleLen-1])
    
        tupleInfo[0] = tupleInfo[0].replace("(","")
        tupleInfo[1] = tupleInfo[1].replace(")","")
        
        if results[lineContents[0]][4][1]<int(tupleInfo[1]):
                    results[lineContents[0]][4] = (tupleInfo[0],int(tupleInfo[1]))
                    results[lineContents[0]] = sorted(results[lineContents[0]], key=lambda value:value[1], reverse=True)
    
        #Add the tuple to a list of tuples that are under the language/project key in the results
        #results[lineContents[0]].append((tupleInfo[0],int(tupleInfo[1])))    
   
    #Sort the results by the page hit number(second element) of the tuple
   # for key in results:
   #     results[key] = sorted(results[key], key=lambda value:value[1] , reverse=True)
    
    
    #Write the top 5 entries for each language/project key to the output file
    for key in results: 
        for index in range(0,num_top_entries):
            if len(results[key])>index and results[key][index][1]>0:
                    output = key+"\t("+results[key][index][0] +","+ str(results[key][index][1])+")\n"
                    output_stream.write(output)
    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, num_top_entries):
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
    my_reduce(my_input_stream, num_top_entries, my_output_stream)

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

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    num_top_entries = 5

    my_main(debug, i_file_name, o_file_name, num_top_entries)
