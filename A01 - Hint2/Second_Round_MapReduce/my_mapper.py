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
def my_map(input_stream, per_language_or_project, output_stream):
    results={}
    fileContents = input_stream.read()
    fileContents = fileContents.splitlines()
    for line in fileContents:
        lineContents=line.split(" ")
        #projLang will be the key ie en,fr or m,wikipeida
        projLang=""
        #value is the number of page hits
        value=0
        if lineContents[0] == "0":
            #True if R to L language
            projLang=lineContents[3].split(".",1)
            value=int(lineContents[1])
        else:
            #True if L to R language
            projLang=lineContents[0].split(".")
            value=int(lineContents[2])
        
        if per_language_or_project:
            #If it's for language
            if projLang[0] not in results:
                #True if it comes across a language that is not already in the dictionary
                #Adds language to dictionary
                results[projLang[0]]=value
            else:
                #True if the language already exists in the dictionary
                #Adds the page hits of the language to the dictionary
                results[projLang[0]]=results[projLang[0]]+value
        else:
            #If it's for project
            if len(projLang)==1:
                #If the length of the language/project is 1 then it's from wikipeida
                if "Wikipedia" not in results:
                    results["Wikipedia"]=value
                else:
                    results["Wikipedia"]=results["Wikipedia"]+value
            elif projLang[1] not in results:
                
                results[projLang[1]]=value
            else:
                results[projLang[1]]=results[projLang[1]]+value
           
    for key in results:
        output=key+"\t"+str(results[key])+"\n"
        output_stream.write(output)
        
    
    
    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
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
    my_map(my_input_stream, per_language_or_project, my_output_stream)

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

    i_file_name = "../../../my_dataset/pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    per_language_or_project = True # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
