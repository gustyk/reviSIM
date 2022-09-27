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
        self.turnOverTime = timedelta(seconds=0)
        self.lateCount = 0
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
        self.num_triggered = 0
        self.processed_item = 0
        self.limitExceeded = False
        self.reduce_start = False
 
    def run_simpy(self, fn, limit):
        # Assign order file to process
        self.fn = fn
        # Assign simulation limit
        self.limit = limit
        # Assign data from order file to list
        self.created_time = fn['Created Time'].to_list()
        self.total_item = fn['Total Item'].to_list()
        self.positions = fn['Position'].to_list()
        # Assign Initial Time
        self.initial_time = self.created_time[0].replace(minute=0, second=0)
        # Loop until all orders in the file is processed or reached time limit
        while self.currentRow < len(self.created_time) and self.env.now < self.limit and not self.limitExceeded:
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
            
            if (self.env.now <= self.limit):
            # if not past time limit
                # Add order item count to total item
                self.total += self.total_item[self.currentRow]
                self.current_pool[0][1] += self.total_item[self.currentRow]
                self.current_pool[0][0] += self.positions[self.currentRow]
            else:
                time_now -= timedelta(seconds=(self.env.now - self.limit))
                self.env.now = self.limit

            # Deleting finished picker list
            if len(self.picker_list) != 0:
                if self.picker_list[0] <= time_now:
                    self.picker_list.pop(0)
            
            # Trigger processing
            if getattr(self, 'opt_' + str(self.opt))() or self.env.now >= self.limit or self.limitExceeded:
                self.num_triggered += 1
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
                for batch in collected_batches:
                    self.processed_item += batch[1]
                    cartUti += round(batch[1]/self.cart_capacity, 2)
                    cartUti = round(cartUti, 2)
                    fCount += 1
                self.cartUtility += round(cartUti/len(collected_batches), 2)
                self.fileCount += fCount
                # Processing routing variation
                self.routing.run(copy.deepcopy(collected_batches))
                calculated_compl_time = self.routing.count_completion_time()
                
                # Counting total completion time
                for idx, fl in enumerate(calculated_compl_time):
                    # continue process if not limit exceeded
                    if (not self.limitExceeded):                        
                        if (len(self.picker_list) == self.picker):
                            # If all picker busy, finish time added to the soonest picker
                            finTime = self.picker_list[0] + fl + timedelta(minutes=1)
                        else:
                            # else assign new picker
                            finTime = startTime + fl + timedelta(minutes=1)

                        # if the expected finish time more than time limit
                        # mark as limit exceeded to halt further processing
                        if finTime > (self.initial_time + timedelta(seconds=self.limit)):
                            self.limitExceeded = True
                        else:
                            self.completionTime += fl

                            if (len(self.picker_list) == self.picker):
                                # update soonest picker finish time
                                self.picker_list[0] = finTime
                            else:
                                # assign new picker
                                self.picker_list.append(finTime)
                            self.picker_list.sort()
                    
                            # Counting Turn Over Time
                            for order in raw_batch[idx].to_numpy():
                                tov_time = (finTime - order[0])
                                self.turnOverTime += tov_time
                                if finTime > order[2]:
                                    self.lateCount += 1

                # Looping condition
                self.startRow = self.currentRow
                self.time_limit += self.delta

            # Advance row to check
            self.currentRow += 1
    
    def run(self, fn, limit):
        # Assign order file to process
        self.fn = fn
        # Assign simulation limit
        self.limit = limit
        # Assign data from order file to list
        self.created_time = fn['Created Time'].to_list()
        self.total_item = fn['Total Item'].to_list()
        self.positions = fn['Position'].to_list()
        # Assign Initial Time
        self.initial_time = self.created_time[0].replace(minute=0, second=0)
        # Loop until all orders in the file is processed or reached time limit
        while self.currentRow < len(self.created_time) and self.env.now < self.limit and not self.limitExceeded:
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
            
            if (self.env.now <= self.limit):
            # if not past time limit
                # Add order item count to total item
                self.total += self.total_item[self.currentRow]
                self.current_pool[0][1] += self.total_item[self.currentRow]
                self.current_pool[0][0] += self.positions[self.currentRow]
            else:
                time_now -= timedelta(seconds=(self.env.now - self.limit))
                self.env.now = self.limit

            # Deleting finished picker list
            if len(self.picker_list) != 0:
                if self.picker_list[0] <= time_now:
                    self.picker_list.pop(0)
            
            # Trigger processing
            if getattr(self, 'opt_' + str(self.opt))() or self.env.now >= self.limit or self.limitExceeded or self.currentRow == (len(self.created_time) - 1):
                self.num_triggered += 1
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
                for batch in collected_batches:
                    self.processed_item += batch[1]
                    cartUti += round(batch[1]/self.cart_capacity, 2)
                    cartUti = round(cartUti, 2)
                    fCount += 1
                self.cartUtility += round(cartUti/len(collected_batches), 2)
                self.fileCount += fCount
                # Processing routing variation
                self.routing.run(copy.deepcopy(collected_batches))
                calculated_compl_time = self.routing.count_completion_time()
                
                # Counting total completion time
                for idx, fl in enumerate(calculated_compl_time):
                    # continue process if not limit exceeded
                    if (not self.limitExceeded):                        
                        if (len(self.picker_list) == self.picker):
                            # If all picker busy, finish time added to the soonest picker
                            finTime = self.picker_list[0] + fl + timedelta(minutes=1)
                        else:
                            # else assign new picker
                            finTime = startTime + fl + timedelta(minutes=1)

                        # if the expected finish time more than time limit
                        # mark as limit exceeded to halt further processing
                        if finTime > (self.initial_time + timedelta(seconds=self.limit)):
                            self.limitExceeded = True
                        else:
                            self.completionTime += fl

                            if (len(self.picker_list) == self.picker):
                                # update soonest picker finish time
                                self.picker_list[0] = finTime
                            else:
                                # assign new picker
                                self.picker_list.append(finTime)
                            self.picker_list.sort()
                    
                            # Counting Turn Over Time
                            for order in raw_batch[idx].to_numpy():
                                tov_time = (finTime - order[0])
                                self.turnOverTime += tov_time
                                if finTime > order[2]:
                                    self.lateCount += 1

                # Looping condition
                self.startRow = self.currentRow
                
                if self.reduce_start:
                    self.current_pool[0][1] += self.total_item[self.startRow]
                    self.current_pool[0][0] += self.positions[self.startRow]
                    self.reduce_start = False
                self.time_limit += self.delta

            # Advance row to check
            self.currentRow += 1

    def opt_1(self):
        # FCFS
        if self.env.now >= self.time_limit or self.currentRow == len(self.created_time):
            if self.env.now > self.time_limit:
                self.reduce_start = True
            return True
        else:
            return False
    def opt_2(self):
        # Seed
        return self.currentRow == self.order_num or self.currentRow == len(self.created_time)
    def opt_3(self):
        # Max Picker
        return (len(self.picker_list) < self.picker and self.currentRow - self.startRow != 0) or self.currentRow == len(self.created_time)
    def opt_4(self):
        # Max Cart
        if (self.current_pool[0][1] >= self.cart_capacity and (self.currentRow - 1) - self.startRow != 0) or self.currentRow == len(self.created_time):
            if self.current_pool[0][1] >= self.cart_capacity:
                self.reduce_start = True
            return True
        else:
            return False
    def opt_5(self):
        # Urgent First + Max Picker
        # check urgent order
        self.check_urgent()
        return (self.urgentCount >= self.maxUrgent and (len(self.picker_list) < self.picker and self.currentRow - self.startRow != 0)) or self.currentRow == len(self.created_time)
    def opt_6(self):
        # Urgent First + Max Cart
        # check urgent order
        self.check_urgent()
        if (self.urgentCount >= self.maxUrgent and (self.current_pool[0][1] >= self.cart_capacity and (self.currentRow - 1) - self.startRow != 0)) or self.currentRow == len(self.created_time):
            if self.current_pool[0][1] >= self.cart_capacity:
                self.reduce_start = True
            return True
        else:
            return False

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