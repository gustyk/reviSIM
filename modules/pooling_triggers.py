import pandas as pd
from datetime import timedelta
import copy

class pooling_triggers:
    def __init__(self, opt, env, picker, batching, routing, cart_capacity, delta):
        self.opt = opt
        self.env = env
        self.picker = picker
        self.batching = batching
        self.routing = routing
        self.cart_capacity = cart_capacity
        self.picker_list = list()
        self.delta = delta
        self.cartUtility = 0
        self.onTime = 0
        self.fileCount = 0
        self.completionTime = timedelta(seconds=0)
        self.startRow = 0
        self.currentRow = 0
        self.time_limit = delta
        self.next_time = 0
        self.order_num = 12
        self.total_item = 0
        self.total = 0
        self.urgentCount = 0
        self.maxUrgent = 2
        self.current_pool = [[[], 0]]
 
    def run_simpy(self, fn):
        # Assign order file to process
        self.fn = fn
        # Assign data from order file to list
        self.created_time = fn['Created Time'].to_list()
        self.total_item = fn['Total Item'].to_list()
        self.positions = fn['Position'].to_list()
        # Assign Initial Time
        self.initial_time = self.created_time[0].replace(minute=0, second=0)
        # Loop until all orders in the file is processed
        while self.currentRow < len(self.created_time):
            # Calculate current timestamp
            time_now = self.created_time[self.currentRow]
            # Calculate next time delta
            if self.currentRow == 0:
                new_start = self.created_time[self.currentRow].replace(minute=0, second=0)
                next_delta = (self.created_time[self.currentRow] - new_start).seconds
            else:
                next_delta = (self.created_time[self.currentRow] - self.created_time[self.currentRow - 1]).seconds
            # Time flow to the current order record
            yield self.env.timeout(next_delta)
            
            # Add order item count to total item
            self.total += self.total_item[self.currentRow]
            self.current_pool[0][1] += self.total_item[self.currentRow]
            self.current_pool[0][0] += self.positions[self.currentRow]

            # Deleting finished picker list
            if len(self.picker_list) != 0:
                if self.picker_list[0] <= time_now:
                    self.picker_list.pop(0)
            
            # Trigger processing
            if getattr(self, 'opt_' + str(self.opt))():
                self.current_pool = [[[], 0]]
                # if self.total > self.cart_capacity:
                #     self.currentRow -= 1
                # select orders to batch
                batch_orders = self.fn.iloc[self.startRow:self.currentRow]
                startTime = batch_orders['Created Time'][-1]
                # Processing batching
                raw_batch = self.batching.run(batch_orders)
                collected_batches = self.batching.collect_batch(raw_batch)
                # Counting cart utility
                cartUti = 0
                fCount = 0
                for file in collected_batches:
                    cartUti += round(file[1]/self.cart_capacity, 2)
                    cartUti = round(cartUti, 2)
                    fCount += 1
                self.cartUtility += round(cartUti, 2)
                self.fileCount += fCount
                # Processing routing variation
                self.routing.run(collected_batches)
                calculated_routing = self.routing.count_completion_time()
                
                # Counting total completion time
                finish_time = list()
                for fl in calculated_routing:
                    self.completionTime += fl
                    
                    if (len(self.picker_list) == self.picker):
                        # If all picker busy, finish time added to the soonest picker
                        finTime = self.picker_list[0] + fl + timedelta(minutes=1)
                    else:
                        # else assign new picker
                        finTime = startTime + fl + timedelta(minutes=1)
                    self.picker_list.append(finTime)
                    self.picker_list.sort()
                    finish_time.append(finTime)
                
                # Counting on time delivery
                finish_index = 0
                while finish_index < len(finish_time):
                    dueTime = raw_batch[finish_index]['Due Time'].to_list()
                    due_index = 0
                    while due_index < len(dueTime):
                        if finish_time[finish_index] < dueTime[due_index]:
                            self.onTime += 1
                        due_index += 1
                    finish_index += 1
                # Looping condition
                self.startRow = self.currentRow
                self.time_limit += self.delta

            # Advance row to check
            self.currentRow += 1
    
    def run(self, fn):
        # Assign order file to process
        self.fn = fn
        # Assign data from order file to list
        self.created_time = fn['Created Time'].to_list()
        self.total_item = fn['Total Item'].to_list()
        self.positions = fn['Position'].to_list()
        # Assign Initial Time
        self.initial_time = self.created_time[0].replace(minute=0, second=0)
        # Loop until all orders in the file is processed
        while self.currentRow < len(self.created_time):
            # Calculate current timestamp
            time_now = self.created_time[self.currentRow]
            # Calculate next time delta
            if self.currentRow == 0:
                new_start = self.created_time[self.currentRow].replace(minute=0, second=0)
                next_delta = (self.created_time[self.currentRow] - new_start).seconds
            else:
                next_delta = (self.created_time[self.currentRow] - self.created_time[self.currentRow - 1]).seconds
            # Time flow to the current order record
            self.env.now += next_delta
            
            # Add order item count to total item
            self.total += self.total_item[self.currentRow]
            self.current_pool[0][1] += self.total_item[self.currentRow]
            self.current_pool[0][0] += self.positions[self.currentRow]

            # Deleting finished picker list
            if len(self.picker_list) != 0:
                if self.picker_list[0] <= time_now:
                    self.picker_list.pop(0)
            
            # Trigger processing
            if getattr(self, 'opt_' + str(self.opt))():
                self.current_pool = [[[], 0]]
                # if self.total > self.cart_capacity:
                #     self.currentRow -= 1
                # select orders to batch
                batch_orders = self.fn.iloc[self.startRow:self.currentRow]
                startTime = batch_orders['Created Time'][-1]
                # Processing batching
                raw_batch = self.batching.run(batch_orders)
                collected_batches = self.batching.collect_batch(raw_batch)
                # Counting cart utility
                cartUti = 0
                fCount = 0
                for file in collected_batches:
                    cartUti += round(file[1]/self.cart_capacity, 2)
                    cartUti = round(cartUti, 2)
                    fCount += 1
                self.cartUtility += round(cartUti, 2)
                self.fileCount += fCount
                # Processing routing variation
                self.routing.run(collected_batches)
                calculated_routing = self.routing.count_completion_time()
                
                # Counting total completion time
                finish_time = list()
                for fl in calculated_routing:
                    self.completionTime += fl
                    
                    if (len(self.picker_list) == self.picker):
                        # If all picker busy, finish time added to the soonest picker
                        finTime = self.picker_list[0] + fl + timedelta(minutes=1)
                    else:
                        # else assign new picker
                        finTime = startTime + fl + timedelta(minutes=1)
                    self.picker_list.append(finTime)
                    self.picker_list.sort()
                    finish_time.append(finTime)
                
                # Counting on time delivery
                finish_index = 0
                while finish_index < len(finish_time):
                    dueTime = raw_batch[finish_index]['Due Time'].to_list()
                    due_index = 0
                    while due_index < len(dueTime):
                        if finish_time[finish_index] < dueTime[due_index]:
                            self.onTime += 1
                        due_index += 1
                    finish_index += 1
                # Looping condition
                self.startRow = self.currentRow
                self.time_limit += self.delta

            # Advance row to check
            self.currentRow += 1

    def opt_1(self):
        # FCFS
        return self.env.now >= self.time_limit or self.currentRow == len(self.created_time)
    def opt_2(self):
        # Seed
        return self.currentRow == self.order_num or self.currentRow == len(self.created_time)
    def opt_3(self):
        # Max Picker
        return (len(self.picker_list) < self.picker and self.currentRow - self.startRow != 0) or self.currentRow == len(self.created_time)
    def opt_4(self):
        # Max Cart
        return (self.total >= self.cart_capacity and (self.currentRow - 1) - self.startRow != 0) or self.currentRow == len(self.created_time)
    def opt_5(self):
        # Urgent First + Max Picker
        # check urgent order
        self.check_urgent()
        return (self.urgentCount >= self.maxUrgent and (len(self.picker_list) < self.picker and self.currentRow - self.startRow != 0)) or self.currentRow == len(self.created_time)
    def opt_6(self):
        # Urgent First + Max Cart
        # check urgent order
        self.check_urgent()
        return (self.urgentCount >= self.maxUrgent and (self.total >= self.cart_capacity and (self.currentRow - 1) - self.startRow != 0)) or self.currentRow == len(self.created_time)

    def check_urgent(self):
        # check urgent order
        to_routing = copy.deepcopy(self.current_pool)
        self.routing.run(to_routing)
        compl_time = self.routing.count_completion_time()
        all_compl_time = sum(c.seconds for c in compl_time)
        self.urgentCount = 0
        for order_due in self.fn.iloc[self.startRow:self.currentRow]['Due Time'].to_list():
            if (self.initial_time + timedelta(seconds=(self.env.now + all_compl_time))) > order_due:
                self.urgentCount += 1