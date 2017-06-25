'''
    This file contains answers to the tasks 
    listed in the tutorial.py file.
'''

import arrow
from math import floor
from pymongo import MongoClient
# uncomment the next line if you want to use authentication
#from secure import MONGO_USERNAME, MONGO_PASSWORD

# SET UP THE CONNECTION

client          = MongoClient("localhost", 27017)
db              = client["aprender"] 
mathcards       = client["mathcards"]

# AUTHENTICATE THE CONNECTION

#client.aprender.authenticate(MONGO_USERNAME, MONGO_PASSWORD, 
#    mechanism='SCRAM-SHA-1')

#  uuid of a randomly-selected user
uuid_of_interest = "urn:uuid:d993cbbf-ebae-4f58-9073-620c5e81a18d"


valid_tasks     = ["1a","1b","2a","2b","3","4","5","6","7","8","9","9a","10"]
choosing_task   = True

def print_results_from(this_pipeline, task_id):
    print("*******")
    print("Pipeline for {} is in progress!!!!!".format(task_id))
    print("*******")
    cursor = db.mathcards.aggregate(this_pipeline)
    for doc in cursor:
        print(doc)

print("*********")
print("MDBW17 Tutorial")
while choosing_task:
    print("Which task are you working on?")
    task_id = input("Type the task id, e.g. 9a > ")
    if task_id in valid_tasks:
        task = "task" + task_id
        choosing_task = False
    else:
        print("INVALID TASK ID.  Please re-type.")


processing_now  = False
using_v344      = False

this_instant    = floor(1000*arrow.utcnow().float_timestamp)    # right now!
this_morning    = this_instant - 1000*12*60*60                  # 12 hours earlier than now

if processing_now:
    this_morning = 0  # for testing 

# retrieve one mathcard --> DICTIONARY

card            = db.mathcards.find_one()


 
# LET'S BUILD SOME INSIGHTFUL AGGREGATION PIPELINES

if "task1a" == task:
    # task1a
    # TASK 1a: Figure out how many sessions happened today

    pipeline1a = [
        
            {"$match": { "session_start": { "$gte": this_morning } } },
            {"$count":  "total_sessions_today" }       
            
    ]

    print(task)
    print("..counting documents from today....")
    cursor1a = db.mathcards.aggregate(pipeline1a)
    for doc in cursor1a:
        print(doc)

if "task1b" == task:
    # task1b
    # TASK 1b: For today's sessions, display all the start times

    pipeline1b = [
        
            {"$match": { "session_start": { "$gte": this_morning } } },

            {"$project": {"session_start": 1, "_id":0}}     
            
    ]

    print("..counting documents from today....")
    cursor1b = db.mathcards.aggregate(pipeline1b)
    for doc in cursor1b:
        print(doc)

if "task2a" == task:
    # task2a
    # TASK 2a: Figure out how many total problems today's users answered
    # --> HINT: use $project

    pipeline2a = [
        
            {"$match": { "session_start": { "$gte": this_morning } } },
            {"$project":  {"session_probs"   : {"$size": "$all_problems"} } }             
        
    ]

    print("...total problems by user by session")
    cursor2a = db.mathcards.aggregate(pipeline2a)
    for doc in cursor2a:
        print(doc)

if "task2b" == task:
    # task2b
    # TASK 2b: Figure out how many total problems today's users answered
    # --> use $project

    pipeline2b = [ 
        
            {"$match"   :   {"session_start"    : {"$gte"   : this_morning } } },
            {"$project" :   {"session_probs"    : {"$size"  : "$all_problems"} } },          
            {"$group"   :   { 
                "_id" : None,
                "total_problems"   : {"$sum"   : "$session_probs"} 
            }}
        
    ]

    print("...counting total problems solved today....")
    cursor2b = db.mathcards.aggregate(pipeline2b)
    for doc in cursor2b:
        print(doc)

if "task3" == task:
    # task3
    # TASK 3: How many problems have been solved by all users for all time

    pipeline3 = [ 
        
            {"$project" :   {"session_probs"    : {"$size"  : "$all_problems"} } },          
            {"$group"   :   { 
                "_id" : None,
                "total_problems"   : {"$sum"   : "$session_probs"} 
            }}
        
    ]

    print("...counting total problems solved for all time....")
    cursor3 = db.mathcards.aggregate(pipeline3)
    for doc in cursor3:
        print(doc)

if "task4" == task:
    # task4
    # TASK 4: Average number of problems solved by today's users

    pipeline4 = [ 
        
            {"$match"   :   {"session_start"    : {"$gte"   : this_morning } } },
            {"$project" :   {"session_probs"    : {"$size"  : "$all_problems"} } },          
            {"$group"   :   {
                "_id" : None,
                "avg_num_probs"     : {"$avg": "$session_probs"}
            }}
        
    ]

    print("...average number of problems solved today....")
    cursor4 = db.mathcards.aggregate(pipeline4)
    for doc in cursor4:
        print(doc)

if "task5" == task:
    # task5
    # TASK 5: Standard deviation of number of problems solved

    pipeline5 = [ 
        
            {"$match"   :   {"session_start"    : {"$gte"   : this_morning } } },
            {"$project" :   {"session_probs"    : {"$size"  : "$all_problems"} } },          
            {"$group"   :   {
                "_id" : None,
                "std_dev_num_probs"     : {"$stdDevSamp": "$session_probs"}
            }}
        
    ]

    print("...std dev of number of problems solved today....")
    cursor5 = db.mathcards.aggregate(pipeline5)
    for doc in cursor5:
        print(doc)

if "task6" == task:
    # task6
    # TASK 6: Calculate response time by operand2 for a specific user for all problems

    
    pipeline6 = [ 
            { "$match"  :   {"uuid"    : uuid_of_interest } }, # output: cursor of doc(s) from the user of interest
            { "$unwind" : "$all_problems" }, # output: a cursor of _individual documents for each element of all_problems_
            { "$project": {
                "operand2": "$all_problems.operand2",
                "time_spent": {"$subtract": ["$all_problems.end_time","$all_problems.start_time"]},
                "session_start":1, 
                "_id":0}
            },
            { "$group":{
                    "_id": {"operand2": "$operand2"},
                    "avg_time_spent": {"$avg": "$time_spent"},
                    }
            }
    ]

    cursor6 = db.mathcards.aggregate(pipeline6)
    for doc in cursor6:
        print(doc)

if "task7a" == task:
    # task7
    # TASK 7: Calculate percent accuracy by operand2 for a specific user for all problems

    uuid_of_interest = "urn:uuid:9773cb59-3f7f-49e2-a965-3c5733522334"  #  uuid of a randomly-selected user

    pipeline7a = [ 
            { "$match"  :   {"uuid"    : uuid_of_interest } }, # output: cursor of doc(s) from the user of interest
            { "$unwind" : "$all_problems" }, # output: a cursor of _individual documents for each element of all_problems_
            { "$group":{
                    "_id": "$all_problems.operand2", 
                    "total_attempted":  {"$sum":1},
                    "total_correct": {"$sum": { "$cond": ["$all_problems.correct", 1, 0] } }
                    }
            },
            { "$addFields":{
                "percent_accuracy": {"$divide": ["$total_correct","$total_attempted"]}
                }
            }
    ]

    print("*******")
    print("Pipeline 7a is in progress!!!!!")
    print("*******")
    cursor7a = db.mathcards.aggregate(pipeline7a)
    for doc in cursor7a:
        print(doc)

pipeline7 = [ 
        { "$match"  :   {"uuid"    : uuid_of_interest } }, # output: cursor of doc(s) from the user of interest
        { "$unwind" : "$all_problems" }, # output: a cursor of _individual documents for each element of all_problems_
        { "$project": {
            "operand2": "$all_problems.operand2",
            "correct": {"$cond":{
                        "if": "$all_problems.correct",
                        "then": 1,
                        "else": 0
                    }},
            "session_start":1, 
            "_id":0}
        },
        { "$group":{
                "_id": {"operand2": "$operand2"},
                "total_attempted":  {"$sum":1},
                "total_correct": {"$sum": "$correct"}
                }
        },
        { "$addFields":{
            "percent_accuracy": {"$divide": ["$total_correct","$total_attempted"]}
            }
        }
]

print("AND HERE'S AN ALT VERSION")
cursor7 = db.mathcards.aggregate(pipeline7)
for doc in cursor7:
    print(doc)

pipeline7b = [ 
        { "$match"  :   {"session_start" : {"$gt": 1496768091131, "$lt": 1496836932400} }},
        { "$unwind" :   "$all_problems" }, # output: a cursor of _individual documents for each element of all_problems_
        { "$group":{
                "_id": { "session_start": "$session_start", "uuid": "$uuid", "operand2": "$all_problems.operand2"  },
                "total_attempted":  {"$sum":1},
                "total_correct": {"$sum": { "$cond": ["$all_problems.correct", 1, 0] } }
                }
        },
        { "$addFields":{
            "percent_accuracy":  {"$divide": ["$total_correct","$total_attempted"]}
            }
        },
        {"$project": {
            "_id": {"uuid": "$_id.uuid", "op2" : "$_id.operand2"},
            "percent_accuracy": 1
            }
        },
        {"$sort":  {"_id.uuid": 1, "_id.op2": 1} }
]

print("*******")
print("Pipeline 7b is in progress!!!!!")
print("*******")
cursor7b = db.mathcards.aggregate(pipeline7b)
for doc in cursor7b:
    print(doc)

# task8
# TASK 8: Retrieve, for one user, operand2 w/ lowest percent accuracy

pipeline7.extend([
        {"$sort": {"percent_accuracy": 1}}, 
        {"$limit": 1}
    ])

print("....")
print("now finding the operand2 with the lowest percent accuracy....")

cursor8 = db.mathcards.aggregate(pipeline7) # pipeline7 is now different than it was in task7

for doc in cursor8:
    print("This user had the most difficulty with {}".format(doc["_id"]["operand2"]))
    print(doc)


if "task9" == task:
    # task9
    # TASK 9: Retrieve, for one user, `operand2` w/ fastest time

    pipeline9 = [ 
            { "$match"  :   {"uuid"    : uuid_of_interest } }, # output: cursor of doc(s) from the user of interest
            { "$unwind" : "$all_problems" }, # output: a cursor of _individual documents for each element of all_problems_
            { "$project": {
                "operand2": "$all_problems.operand2",
                "time_spent": {"$subtract": ["$all_problems.end_time","$all_problems.start_time"]},
                "session_start":1, 
                "_id":0}
            },
            { "$group":{
                    "_id": {"operand2": "$operand2"},
                    "avg_time_spent": {"$avg": "$time_spent"},
                    }
            },
            {"$sort": {"avg_time_spent_milliseconds": 1}},
            {"$limit": 1}
    ]

    print("*******")
    print("Pipeline 9 is in progress!!!!!")
    print("*******")
    cursor9 = db.mathcards.aggregate(pipeline9)
    for doc in cursor9:
        print(doc)



if "task9a" == task:
    pipeline9a = [ 
            { "$match"  :   {"session_start" : {"$gt": 1496768091131, "$lt": 1496836932400} }},
            { "$unwind" :   "$all_problems" }, # output: a cursor of _individual documents for each element of all_problems_
            { "$group":{
                    "_id": { "session_start": "$session_start", "uuid": "$uuid", "operand2": "$all_problems.operand2"  },
                    "op2": { "$first": "$all_problems.operand2"},
                    "total_attempted":  {"$sum":1},
                    "total_correct": {"$sum": { "$cond": ["$all_problems.correct", 1, 0] } }
                    }
            },
            { "$group":{
                "_id": "$_id.uuid",
                "op_scores": {"$push": {"op": "$op2", "score": {"$trunc": {"$multiply": [100, {"$divide": ["$total_correct","$total_attempted"]}]}}  } },
                }
            }
    ]

    print_results_from(pipeline9a, task)


if "task10" == task:

    pipeline10 = [ 
            # $unwind: create individual documents for each item in the all_problems list
            { "$unwind"     :   "$all_problems" }, 
            { "$group":{
                    "_id": {"op2": "$all_problems.operand2"},
                    "total_attempted"   :  {"$sum": 1},
                    "total_correct"     :  {"$sum": { "$cond": ["$all_problems.correct", 1, 0] } }
                    }
            },
            { "$addFields":{
                "percent_accuracy":  {"$divide": ["$total_correct","$total_attempted"]}  
                }
            },
            {"$sort"    :  {"percent_accuracy": 1} }
    ]


    print("finding the number which was 'weakest' for the most users")
    print_results_from(pipeline10, task)

"""
# this is a pretty advanced aggregation pipeline--
# but check it out anyway!

if using_v344:

    pipeline8 = [
        {"$match":  { "interventions": {"$exists": True}}},
        {"$addFields":          # new field measuring whether user likes difficulty
            {"seeks_challenge": 
                {"$in":[True,
                    {"$map":{   # run a `map` stage on every doc
                        "input" : {"$range":[0,{"$subtract":[{"$size":"$interventions"},1]}]},
                        "as"    : "z",          # `z` is the loop variable; the index in the array
                        "in"    : {"$let":{ 
                                        "vars": {
                                            "nth"   :{"$arrayElemAt":["$interventions","$$z"]},
                                            "nplus1":{"$arrayElemAt":["$interventions", {"$add":[1,"$$z"]}]}
                                        },
                                        "in":   {"$cond":{
                                                    "if": {"$and":[ {"$eq": ["$$nth.label", "tryHarder"]},
                                                                    {"$eq": ["$$nth.user_response", "yes"]},
                                                                    {"$eq": ["$$nplus1.label", "tryHarder"]},
                                                                    {"$eq": ["$$nplus1.user_response", "yes"]} ]},
                                                    "then": True,
                                                    "else": False   
                                                }},
                        }},

                    }}
                ]}

            }

        },
        {"$match": {"seeks_challenge":True}}
    ]

    agg8 = db.mathcards.aggregate(pipeline8)
    print("PIPELINE 8 to measure which users seek challenge")
    for doc in agg8:
        print(doc["_id"])


"""

       


