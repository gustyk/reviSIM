from typing import Dict
import time
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
# order_file = list(range(1, a)) + ['Avg']
order_file = list()
# Counting total order
# total_order = gf.count_total_order(fname)
total_order = list()
# Counting total item picked
# total_item_picked = gf.count_total_item(fname)
total_item_picked = list()

# Specify order limit for trigger VTWB
max_order = 12
# Specify urgent order limit
max_urgent = 1

trigger_list = list()
batching_list = list()
routing_list = list()
picker_list = list()
cart_list = list()

total_completion_time = list()
total_turn_over_time = list()
average_turn_over_time = list()
average_picker_utility = list()
average_cart_utility = list()
late_delivery = list()

for trigger_opt in [1,2,3,4,5,6]:
    for batching_opt in [1,2]:
        for routing_opt in [1,2]:
            for picker_num in [1,2,3,4,5,6,7,8,9,10,11,12]:
                for cart_opt in [1,2,3]:
                    print('Processing rules %d-%d-%d-%d-%d' % (trigger_opt, batching_opt, routing_opt, picker_num, cart_opt))
                    start_time = time.time()
                    cart_capacity = 0
                    if cart_opt == 1:
                        cart_capacity = 50
                    elif cart_opt == 2:
                        cart_capacity = 100
                    elif cart_opt == 3:
                        cart_capacity = 200
                    fn_idx = 1
                    limit = timedelta(hours=8).seconds
                    delta = timedelta(minutes=12).seconds
                    class Object(object):
                        pass

                    env = Object()
                    for fn in fname:    

                        env.now = 0

                        batching = batchings.batchings(batching_opt, cart_capacity)
                        routing = routings.routings(routing_opt)
                        trigger = pooling_triggers.pooling_triggers(trigger_opt, env, picker_num, batching, routing, cart_capacity, delta, max_urgent, max_order)

                        trigger.run(fn, limit)

                        # env.run(until=limit)

                        # Listing total order
                        total_order.append(trigger.current_row - 1)
                        # Listing total total time
                        total_item_picked.append(sum(trigger.total_item))
                        # Listing total completion time
                        total_completion_time.append(round(trigger.completion_time.total_seconds()/60, 2))
                        # Listing total Turonver time
                        total_turn_over_time.append(round(trigger.turn_over_time.total_seconds()/60, 2))
                        # Listing avg Turonver time
                        average_turn_over_time.append(round(trigger.turn_over_time.total_seconds()/60/trigger.current_row, 2))
                        # Counting and listing average picker utility
                        average_picker_utility.append(trigger.ave_picker_utility)
                        # Counting & listing average cart utility
                        ave_cart_utility = round(trigger.cart_utility/trigger.num_triggered, 2)
                        average_cart_utility.append(ave_cart_utility)
                        # Listing total on time delivery
                        late_delivery.append(trigger.late_count)

                        order_file.append(fn_idx)
                        trigger_list.append(trigger_opt)
                        batching_list.append(batching_opt)
                        routing_list.append(routing_opt)
                        picker_list.append(picker_num)
                        cart_list.append(cart_capacity)
                        fn_idx += 1

                    # # Counting average of total order
                    # total_order = gf.count_average(total_order)
                    # # Counting average of total item picked
                    # total_item_picked = gf.count_average(total_item_picked)
                    # # Counting average of total completion time
                    # total_completion_time = gf.average_tct(total_completion_time)
                    # # Counting average of turnover time
                    # total_turn_over_time = gf.average_tct(total_turn_over_time)
                    # # Counting average of average picker utility
                    # average_picker_utility = gf.count_average(average_picker_utility)
                    # # Counting average of average cart utility
                    # average_cart_utility = gf.count_average(average_cart_utility)
                    # # Counting average of on time delivery
                    # late_delivery = gf.count_average(late_delivery)

# Put result on file
result = pd.DataFrame({
    'OrderFile': order_file,
    'TriggerMethod': trigger_list,
    'BatchingMethod': batching_list,
    'RoutingPolicy': routing_list,
    'NumOfPickers': picker_list,
    'CartCapacity': cart_list,
    'TotalOrder': total_order,
    'CompletionTime': total_completion_time,
    'TurnOverTime': total_turn_over_time,
    'AvgTurnOverTime': average_turn_over_time,
    'TotalItemPicked': total_item_picked,
    'AvgPickerUtil': average_picker_utility,
    'AvgCartUtil': average_cart_utility,
    'NumOfLate': late_delivery
    }, columns = ['OrderFile','TriggerMethod','BatchingMethod','RoutingPolicy','NumOfPickers','CartCapacity','TotalOrder','CompletionTime','TurnOverTime', 'AvgTurnOverTime','TotalItemPicked','AvgPickerUtil','AvgCartUtil','NumOfLate'])
result.to_csv('result/All.csv', index = False)
print('All.csv')