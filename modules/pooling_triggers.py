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
        self.order_num_limit = 0
        self.order_num = 12
        self.total_item = 0
        self.total = list()
        self.urgentCount = 0
        self.maxUrgent = 2
        self.current_pool = [[[], 0, []]]
        self.num_triggered = 0
        self.processed_item = 0
        self.limitExceeded = False
        self.batch_orders = list()
        self.back_order = 0
 
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
        self.order_num_limit += self.order_num
        # Loop until all orders in the file is processed or reached time limit
        while (self.currentRow < len(self.created_time) and self.env.now <= self.limit and not self.limitExceeded) or self.current_pool[0][1] > 0:
            # Calculate current timestamp
            time_now = self.initial_time + timedelta(seconds=self.env.now)
            # Calculate next time delta
            
            if (self.currentRow < len(self.created_time) and time_now > self.created_time[self.currentRow]):
                self.currentRow += 1

            while (self.currentRow < len(self.created_time) and time_now == self.created_time[self.currentRow]):
                self.current_pool[0][0] += self.positions[self.currentRow]
                self.current_pool[0][1] += self.total_item[self.currentRow]
                self.current_pool[0][2].append(self.total_item[self.currentRow])
                self.batch_orders.append(self.fn.iloc[self.currentRow])
                self.total.append(self.total_item[self.currentRow])
                self.currentRow += 1

            # Deleting finished picker list
            if len(self.picker_list) != 0:
                if self.picker_list[0] <= time_now:
                    self.picker_list.pop(0)
            
            # Trigger processing
            if getattr(self, 'opt_' + str(self.opt))() or self.env.now >= self.limit or self.limitExceeded or (self.currentRow >= len(self.created_time) and self.current_pool[0][1] > 0):
                self.num_triggered += 1
                self.order_num_limit += self.order_num
                if self.back_order > 0:
                    batch_orders = pd.DataFrame(self.batch_orders[:-self.back_order])
                    self.batch_orders = self.batch_orders[-self.back_order:]
                    remaining_orders = pd.DataFrame(self.batch_orders)
                    item_qty = remaining_orders['Total Item'].to_list()
                    self.current_pool[0][0] = self.current_pool[0][0][-self.back_order:]
                    self.current_pool[0][1] = sum(item_qty)
                    self.current_pool[0][2] = item_qty
                
                else:
                    batch_orders = pd.DataFrame(self.batch_orders)
                    self.batch_orders = list()
                    self.current_pool = [[[], 0, []]]
                    # self.currentRow -= 1

                # select orders to batch
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
                            finTime = time_now + fl + timedelta(minutes=1)

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
                
                if self.back_order > 0:
                    self.back_order = 0

                self.time_limit += self.delta

            # Advance time 1 second
            self.env.now += 1

    def opt_1(self):
        # FCFS
        return self.env.now == self.time_limit or self.currentRow == len(self.created_time)
    def opt_2(self):
        # Seed
        return self.currentRow == self.order_num_limit or self.currentRow == len(self.created_time)
    def opt_3(self):
        # Max Picker
        return (len(self.picker_list) < self.picker and self.currentRow - self.startRow != 0) or self.currentRow == len(self.created_time)
    def opt_4(self):
        # Max Cart
        if (self.current_pool[0][1] >= self.cart_capacity and (self.currentRow - 1) - self.startRow != 0) or self.currentRow == len(self.created_time):
            if self.current_pool[0][1] > self.cart_capacity:
                self.back_order += 1
                while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                    self.back_order += 1
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
            if self.current_pool[0][1] > self.cart_capacity:
                self.back_order += 1
                while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                    self.back_order += 1
            return True
        else:
            return False

    def check_urgent(self):
        # check urgent order
        if self.current_pool[0][1] == 0:
            self.urgentCount = 0
            return 0
        to_routing = copy.deepcopy(self.current_pool)
        self.routing.run(to_routing)
        compl_time = self.routing.count_completion_time()
        all_compl_time = sum(c.seconds for c in compl_time)
        self.urgentCount = 0
        for order_due in self.fn.iloc[self.startRow:self.currentRow]['Due Time'].to_list():
            if (self.initial_time + timedelta(seconds=(self.env.now + all_compl_time))) > order_due:
                self.urgentCount += 1