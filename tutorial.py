''' 
    tutorial.py
    -----------
    This file contains partially-completed aggregation pipelines 
    for use in learning/teaching PyMongo.

    REQUIREMENTS:

        * MongoDB version 3.4.x or later
        * Python environment w/ pymongo
            `pip install pymongo`
        * Follow Handout1 instructions to load sample data
            (from the file `mathcards.bson`)
'''

# import arrow
from math import floor
from pymongo import MongoClient
from secure import MONGO_USERNAME, MONGO_PASSWORD

# SET UP THE CONNECTION

client          = MongoClient("localhost", 27017)
db              = client["aprender"] 
mathcards       = client["mathcards"]

# AUTHENTICATE THE CONNECTION

client.aprender.authenticate(MONGO_USERNAME, MONGO_PASSWORD, 
    mechanism='SCRAM-SHA-1')

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
    task_id = input("Type the task id, e.g. 2a > ")
    task_id = task_id.lower()
    if task_id in valid_tasks:
        task = "task" + task_id
        choosing_task = False
    else:
        print("INVALID TASK ID.  Please re-type.")




#this_instant    = floor(1000*arrow.utcnow().float_timestamp)    # right now!
#this_morning    = this_instant - 1000*12*60*60                  # 12 hours earlier than now


this_morning = 0  # the UNIX epoch!

# retrieve one mathcard --> DICTIONARY

card            = db.mathcards.find_one()

# retrieve all mathcards --> CURSOR
# all_cards       = db.mathcards.find()       

 
# cards greater than or equal to a certain timestamp

all_cards    = db.mathcards.find( { "session_start": { "$gte": 0 } } )


 
# BUILD SOME GROOVY AND INSIGHTFUL PIPELINES

if "task1a" == task:
    # task1a
    # TASK 1a: Figure out how many sessions happened today

    pipeline1a = [
        
            {"$match": { "session_start": { "$gte": this_morning } } },
            {"$count":  "total_sessions_today" }       
            
    ]

    print(task)
    print("..counting documents from today....")
    cursor1a = db.processed.aggregate(pipeline1a)
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
    cursor1b = db.processed.aggregate(pipeline1b)
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
    cursor2a = db.processed.aggregate(pipeline2a)
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
    cursor2b = db.processed.aggregate(pipeline2b)
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
    cursor3 = db.processed.aggregate(pipeline3)
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
    print_results_from(pipeline4, task)

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
    print_results_from(pipeline5, task)

if "task6" == task:
    # task6
    # TASK 6: Calculate response time by operand2 for a specific user for all problems

    
    pipeline6 = [ 
            { "$match"  :  # $match on uuid_of_interest
            { "$unwind" :  # $unwind array of problems
            { "$project": {
                "operand2":   # use dot notation 
                "time_spent": # compute time spent
            },
            { "$group":{
                    "_id":  # group on operand2
                    "avg_time_spent": # compute $avg 
                    }
            }
    ]

    print_results_from(pipeline6, task)

if "task7a" == task:
    # task7
    # TASK 7: Calculate percent accuracy by operand2 for a specific user for all problems

    uuid_of_interest = "urn:uuid:9773cb59-3f7f-49e2-a965-3c5733522334"  #  uuid of a randomly-selected user

    pipeline7a = [ 
            { "$match"  :  # ...
            { "$unwind" :  # ...
            { "$group":{
                    "_id": # $group on operand2
                    "total_attempted":  # $sum
                    "total_correct": 
                    }
            },
            { "$addFields":{
                "percent_accuracy": 
                }
            }
    ]

    print_results_from(pipeline7a, task)

if "task8" == task:
    # task8
    # TASK 8: Retrieve, for one user, operand2 w/ lowest percent accuracy

    pipeline8 = [

        # task8 code goes here

    ]

    print("....")
    print("now finding the operand2 with the lowest percent accuracy....")

    cursor8 = db.mathcards.aggregate(pipeline8) 

    print_results_from(pipeline8, task)


if "task9" == task:
    # task9
    # TASK 9: Retrieve, for one user, `operand2` w/ fastest time

    pipeline9 = [ 
        #task9 code goes here
    ]

    print_results_from(pipeline9, task)



# Additional sample code related to task9
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

            # task10 code goes here

            # reminder:
            # $unwind: creates individual documents for each item in the all_problems list

    ]


    print("finding the number which was 'weakest' for the most users")
    print_results_from(pipeline10, task)


'''
END OF FILE COMMENTS: a few syntax examples 
and a complicated aggregation pipeline


Note: $project can be used to project/suppress fields.
This example projects `uuid` but gets rid of `_id`:

{"$project": 
            
            "uuid":1,
            "_id":0
            
}

Boolean operator $and

{"$and": [ {},{},{},{} ]}

Conditional operator $cond

{"$cond":{
    "if": {"$and": [ {},{},{},{} ]},
    "then": true,
    "else": false
}}

More detailed example of $cond, as seen in super_pipeline

{"$cond":{
    "if": {"$and":[ {"$eq": ["$$nth.label", "tryHarder"]},
                    {"$eq": ["$$nth.user_response", "yes"]},
                    {"$eq": ["$$nplus1.label", "tryHarder"]},
                    {"$eq": ["$$nplus1.user_response", "yes"]} ]},
    "then": True,
    "else": False   
}}
'''

"""
# bonus
# a complicated agg pipeline example
# for you to study 


studying_super_pipeline = False

if studying_super_pipeline:

    super_pipeline = [
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

    bonus_cursor = db.processed.aggregate(super_pipeline)
    print("SUPER PIPELINE to measure which users seek challenge")
    for doc in bonus_cursor:
        print(doc["_id"])  # print the `ObjectId` values

"""


