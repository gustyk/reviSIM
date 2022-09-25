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
# totalOrder = gf.count_total_order(fname)
totalOrder = list()
# Counting total item picked
# totalItemPicked = gf.count_total_item(fname)
totalItemPicked = list()

trigger_opt = 1
batching_opt = 1
routing_opt = 1
picker_num = 1
cart_opt = 1

for trigger_opt in [1,2,3,4,5,6]:
    for batching_opt in [1,2]:
        for routing_opt in [1,2]:
            for picker_num in [1,2,3,4,5,6,7,8,9,10,11,12]:
                for cart_opt in [1,2,3]:
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

                        limit = timedelta(hours=8).seconds
                        trigger.run(fn, limit)

                        # env.run(until=limit)

                        # Listing total order
                        totalOrder.append(trigger.currentRow - 1)
                        # Listing total total time
                        totalItemPicked.append(trigger.processed_item)
                        # Listing total completion time
                        totalCompletionTime.append(trigger.completionTime)
                        # Listing total Turonver time
                        totalTurnOverTime.append(trigger.turnOverTime)
                        # Counting and listing average picker utility
                        avePickerUtility = round(trigger.completionTime/(timedelta(hours=8)*picker_num), 2)
                        averagePickerUtility.append(avePickerUtility)
                        # Counting & listing average cart utility
                        aveCartUtility = round(trigger.cartUtility/trigger.num_triggered, 2)
                        averageCartUtility.append(aveCartUtility)
                        # Listing total on time delivery
                        lateDelivery.append(trigger.lateCount)

                    # Counting average of total order
                    totalOrder = gf.count_average(totalOrder)
                    # Counting average of total item picked
                    totalItemPicked = gf.count_average(totalItemPicked)
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
                        'TriggerMethod': [trigger_opt] * a,
                        'BatchingMethod': [batching_opt] * a,
                        'RoutingPolicy': [routing_opt] * a,
                        'NumOfPickers': [picker_num] * a,
                        'CartCapacity': [cart_capacity] * a,
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