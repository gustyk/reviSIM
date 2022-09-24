from typing import Dict
import simpy
import pandas as pd
import random
from datetime import timedelta
from modules import general_function as gf

from modules import pooling_triggers
from modules import batchings
from modules import routings
# Reading order file
fname = gf.reading_file()

# Processing file
# Making Index
a = len(fname) + 1
orderFile = list(range(1, a)) + ['Avg']
# Counting total order
totalOrder = gf.count_total_order(fname)
# Counting total item picked
totalItemPicked = gf.count_total_item(fname)

trigger_opt = 1
batching_opt = 1
routing_opt = 1
picker_num = 1
cart_opt = 1

for trigger_opt in [1,2,3,4]:
    for batching_opt in [1,3]:
        for routing_opt in [1,3]:
            for picker_num in range(1,13):
                for cart_opt in [1,3]:
                    cart_capacity = 0
                    if cart_opt == 1:
                        cart_capacity = 50
                    elif cart_opt == 2:
                        cart_capacity = 100
                    elif cart_opt == 3:
                        cart_capacity = 200

                    totalCompletionTime = list()
                    totalTurnOverTime = list()
                    averagePickerUtility = list()
                    averageCartUtility = list()
                    lateDelivery = list()

                    for fn in fname:    
                        delta = timedelta(minutes=12).seconds

                        class Object(object):
                            pass

                        env = Object()
                        env.now = 0

                        batching = batchings.batchings(batching_opt, cart_capacity)
                        routing = routings.routings(batching_opt)
                        trigger = pooling_triggers.pooling_triggers(trigger_opt, env, picker_num, batching, routing, cart_capacity, delta)

                        trigger.run(fn)

                        # limit = timedelta(hours=9).seconds
                        # env.run(until=limit)

                        # Listing total completion time
                        totalCompletionTime.append(trigger.completionTime)
                        # Listing total Turonver time
                        totalTurnOverTime.append(trigger.turnOverTime)
                        # Counting and listing average picker utility
                        avePickerUtility = round(trigger.completionTime/timedelta(hours=8), 2)
                        averagePickerUtility.append(avePickerUtility)
                        # Counting & listing average cart utility
                        aveCartUtility = round(trigger.cartUtility/trigger.fileCount, 2)
                        averageCartUtility.append(aveCartUtility)
                        # Listing total on time delivery
                        lateDelivery.append(trigger.lateCount)

                    # Counting average of total completion time
                    totalCompletionTime = gf.average_tct(totalCompletionTime)
                    # Counting average of turnover time
                    totalTurnOverTime = gf.average_tct(totalTurnOverTime)
                    # Counting average of average picker utility
                    averagePickerUtility = gf.count_average(averagePickerUtility)
                    # Counting average of average cart utility
                    averageCartUtility = gf.count_average(averageCartUtility)
                    # Counting average of on time delivery
                    lateDelivery = gf.count_average(lateDelivery)

                    # Put result on file
                    result = pd.DataFrame({
                        'OrderFile': orderFile,
                        'TriggerMethod': [trigger_opt] * 11,
                        'BatchingMethod': [batching_opt] * 11,
                        'RoutingPolicy': [routing_opt] * 11,
                        'NumOfPickers': [picker_num] * 11,
                        'CartCapacity': [cart_capacity] * 11,
                        'TotalOrder': totalOrder,
                        'CompletionTime': totalCompletionTime,
                        'TurnOverTime': totalTurnOverTime,
                        'TotalItemPicked': totalItemPicked,
                        'AvgPickerUtil': averagePickerUtility,
                        'AvgCartUtil': averageCartUtility,
                        'NumOfLate': lateDelivery
                        }, columns = ['OrderFile','TriggerMethod','BatchingMethod','RoutingPolicy','NumOfPickers','CartCapacity','TotalOrder','CompletionTime','TurnOverTime','TotalItemPicked','AvgPickerUtil','AvgCartUtil','NumOfLate'])
                    filename = str(trigger_opt) + str(batching_opt) + str(routing_opt) + str(picker_num) + str(cart_opt) + '.csv'
                    result.to_csv('result/test' + filename, index = False)
                    print(filename)