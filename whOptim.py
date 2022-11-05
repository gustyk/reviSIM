import time
import simpy
import pandas as pd
import random
from datetime import timedelta
from modules import general_function as gf

from modules import pooling_triggers
from modules import triggers
from modules import batchings
from modules import routings
# Reading order file
fname = gf.reading_file()

# Processing file
# Making Index
a = len(fname) + 1
order_file = list()
# Counting total order
total_order = list()
# Counting total item picked
total_item_picked = list()

# Specify urgent threshold in seconds
urgent_threshold = 300

trigger_list = list()
batching_list = list()
routing_list = list()
picker_list = list()
cart_list = list()

total_completion_time = list()
average_completion_time = list()
total_turn_over_time = list()
average_turn_over_time = list()
average_picker_utility = list()
average_cart_utility = list()
total_tardy_order = list()
total_lateness = list()
average_lateness = list()
total_batches = list()

time_limit = timedelta(hours=8).seconds

def order_stream(trigger_opt, batching_opt, routing_opt, picker_number, cart_capacity):
    fn_idx = 1
    for fn in fname:    
        env = simpy.Environment()
        picker_pool = simpy.Resource(env, capacity=picker_number)
        routing = routings.routings(routing_opt)
        batching = batchings.batchings(batching_opt, cart_capacity, routing, urgent_threshold)
        trigger = triggers.triggers(env, picker_pool, batching, routing, cart_capacity, urgent_threshold)

        trigger.prepare(fn)

        if trigger_opt == 1:
            env.process(trigger.ftwb())
        elif trigger_opt == 2:
            env.process(trigger.vtwb())
        elif trigger_opt == 3:
            env.process(trigger.max_picker())
        elif trigger_opt == 4:
            env.process(trigger.max_cart())
        elif trigger_opt == 5:
            env.process(trigger.ug_max_picker())
        elif trigger_opt == 6:
            env.process(trigger.ug_max_cart())

        # trigger.run(fn, limit)
        env.run(until=time_limit)

        # Listing total order
        total_order.append(trigger.processed_order)
        # Listing total total time
        total_item_picked.append(trigger.processed_item)
        # Listing total completion time
        total_completion_time.append(round(trigger.completion_time.total_seconds()/60, 2))
        # Listing total Turonver time
        total_turn_over_time.append(round(trigger.turn_over_time.total_seconds()/60, 2))
        # Counting and listing average picker utility
        ave_picker_utility = round(trigger.completion_time/(timedelta(hours=8)*picker_number), 2)
        average_picker_utility.append(ave_picker_utility)
        # Listing total on time delivery
        total_tardy_order.append(trigger.tardy_order + len(trigger.current_pool[0][2]))
        # Listing Total Lateness
        total_lateness.append(round(trigger.total_lateness.total_seconds()/60, 2))
        # Listing Total Batches Processed
        total_batches.append(trigger.total_batch)

        if (trigger.processed_order > 0):
            # Listing average completion time
            average_completion_time.append(round(trigger.completion_time.total_seconds()/60/(trigger.processed_order), 2))
            # Listing avg Turonver time
            average_turn_over_time.append(round(trigger.turn_over_time.total_seconds()/60/(trigger.processed_order), 2))
            # Counting & listing average cart utility
            ave_cart_utility = round(trigger.cart_utility/trigger.total_batch, 2)
            average_cart_utility.append(ave_cart_utility)
            # Listing Average Lateness
            average_lateness.append(round(trigger.total_lateness.total_seconds()/(trigger.processed_order)/60,2))
        else:
            average_completion_time.append(0)
            average_turn_over_time.append(0)
            average_cart_utility.append(0)
            average_lateness.append(0)

        order_file.append(fn_idx)
        trigger_list.append(trigger_opt)
        batching_list.append(batching_opt)
        routing_list.append(routing_opt)
        picker_list.append(picker_number)
        cart_list.append(cart_capacity)

        fn_idx += 1

        if (trigger.print_io_station):
            dfIoS = pd.DataFrame(trigger.print_ioStation, columns =['TriggerID', 'OrderIDs', 'BatchID', 'OrderCount', 'StartingTime', 'FinishingTime'])
            pd.set_option('display.max_rows', None)
            print('IoStation Content')
            print(dfIoS)

def result_generator():
    trigger_opts = [1,2,3,4,5,6]
    batching_opts = [1,2]
    routing_opts = [1,2]
    picker_numbers = [1,2,3,4,5,6,7,8,9,10,11,12]
    cart_opts = [1,2,3]

    for trigger_opt in trigger_opts:
        for batching_opt in batching_opts:
            for routing_opt in routing_opts:
                for picker_number in picker_numbers:
                    for cart_opt in cart_opts:
                        cart_capacity = 0
                        if cart_opt == 1:
                            cart_capacity = 50
                        elif cart_opt == 2:
                            cart_capacity = 100
                        elif cart_opt == 3:
                            cart_capacity = 200

                        print('Processing rules %d-%d-%d-%d-%d' % (trigger_opt, batching_opt, routing_opt, picker_number, cart_opt))
                        order_stream(trigger_opt, batching_opt, routing_opt, picker_number, cart_capacity)
    
    # Put result on file
    result = pd.DataFrame({
        'OrderFile': order_file,
        'TriggerMethod': trigger_list,
        'BatchingMethod': batching_list,
        'RoutingPolicy': routing_list,
        'NumOfPickers': picker_list,
        'CartCapacity': cart_list,
        'TotalOrder': total_order,
        'TotalBatch': total_batches,
        'CompletionTime': total_completion_time,
        'AvgCompletionTime': average_completion_time,
        'TurnOverTime': total_turn_over_time,
        'AvgTurnOverTime': average_turn_over_time,
        'TotalItemPicked': total_item_picked,
        'AvgPickerUtil': average_picker_utility,
        'AvgCartUtil': average_cart_utility,
        'NumOfLate': total_tardy_order,
        'TotalLateness': total_lateness,
        'AvgLateness': average_lateness
        }, columns = ['OrderFile','TriggerMethod','BatchingMethod','RoutingPolicy','NumOfPickers','CartCapacity','TotalOrder','TotalBatch','CompletionTime','AvgCompletionTime','TurnOverTime', 'AvgTurnOverTime','TotalItemPicked','AvgPickerUtil','AvgCartUtil','NumOfLate','TotalLateness','AvgLateness'])
    result.to_csv('result/All.csv')
    print('All.csv')

result_generator()