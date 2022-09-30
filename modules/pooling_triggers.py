import pandas as pd
import time
from datetime import timedelta
import copy

class pooling_triggers:
    def __init__(self, opt, env, picker, batching, routing, cart_capacity, delta, max_urgent, max_order):
        self.opt = opt
        self.env = env
        self.picker = picker
        self.batching = batching
        self.routing = routing
        self.cart_capacity = cart_capacity
        self.picker_list = list()
        self.delta = delta
        self.cart_utility = 0
        self.onTime = 0
        self.file_count = 0
        self.completion_time = timedelta(seconds=0)
        self.turn_over_time = timedelta(seconds=0)
        self.late_count = 0
        self.start_row = 0
        # start from the second row
        self.current_row = 1
        self.time_limit = delta
        self.next_time = 0
        self.max_order_limit = 0
        self.max_order = max_order
        self.total_item = 0
        self.urgent_count = 0
        self.max_urgent = max_urgent
        self.current_pool = [[[], 0, []]]
        self.num_triggered = 0
        self.processed_item = 0
        self.limit_exceeded = False
        self.batch_orders = list()
        self.back_order = 0
        self.all_compl_time = 0
        self.last_checked_row = 0
        self.sorted_due_list = list()
 
    def run(self, order_streams, limit):
        # Start time if need to track timelapse
        # start_time = time.time()
        
        # Assign order file to process
        self.order_streams = order_streams
        self.order_streams = order_streams.to_numpy()
        
        # Assign simulation limit
        self.limit = limit
        
        # Assign data from order file to list
        self.created_time = self.order_streams[:,0]
        self.total_item = self.order_streams[:,1]
        self.positions = self.order_streams[:,3]
        self.total_order = len(self.created_time)
        
        # Assign Initial Time
        self.initial_time = self.created_time[0].replace(minute=0, second=0)
        self.max_order_limit += self.max_order
        
        # pre-advance now
        self.env.now += (self.created_time[0] - self.initial_time).seconds
        
        # pre-fill pool
        self.current_pool[0][0] += self.positions[0]
        self.current_pool[0][1] += self.total_item[0]
        self.current_pool[0][2].append(self.total_item[0])
        # Loop until all orders in the file is processed or reached time limit
        while (self.current_row < self.total_order and self.env.now <= self.limit and not self.limit_exceeded) or self.current_pool[0][1] > 0:
            # Calculate current timestamp
            time_now = self.created_time[self.current_row]
            self.env.now += (time_now - self.created_time[self.current_row - 1]).seconds

            # Put current order data to pool cache
            self.current_pool[0][0] += self.positions[self.current_row]
            self.current_pool[0][1] += self.total_item[self.current_row]
            self.current_pool[0][2].append(self.total_item[self.current_row])

            # Deleting finished picker list
            if len(self.picker_list) != 0:
                if self.picker_list[0] <= time_now:
                    self.picker_list.pop(0)
            
            # Trigger processing
            if self.check_trigger() or self.env.now >= self.limit or self.limit_exceeded or (self.current_row >= self.total_order and self.current_pool[0][1] > 0):
                self.num_triggered += 1
                
                # Next order limit
                self.max_order_limit += self.max_order
                
                # Print process detail if required
                # print("%d. Start Row: %d, Current Row: %d, Back Order: %d, secs: %f" % (self.num_triggered, self.start_row, self.current_row, self.back_order, time.time() - start_time))

                # If need to go back one order
                if self.back_order > 0:
                    # Shift order batch taking minus back order count
                    if (self.start_row < self.current_row):
                        batch_orders = self.order_streams[max(0, self.start_row-self.back_order):self.current_row-self.back_order]
                    else:
                        batch_orders = self.order_streams[max(0, self.start_row-self.back_order):self.current_row+1-self.back_order]

                    # Update order pool cache
                    item_qty = self.total_item[self.current_row-self.back_order:self.current_row]
                    self.current_pool[0][0] = self.current_pool[0][0][-self.back_order:]
                    self.current_pool[0][1] = sum(item_qty)
                    self.current_pool[0][2] = item_qty.tolist()
                
                # No need to go back order, take complete batch
                # and empty order pool cache
                else:
                    if (self.start_row < self.current_row):
                        batch_orders = self.order_streams[self.start_row:self.current_row]
                    else:
                        batch_orders = self.order_streams[self.start_row:self.current_row+1]
                    self.current_pool = [[[], 0, []]]

                # Processing batching
                raw_batch = self.batching.run(batch_orders)
                collected_batches = self.batching.collect_batch(raw_batch)

                # Counting cart utility
                cart_uti = 0
                f_count = 0
                for batch in collected_batches:
                    self.processed_item += batch[1]
                    cart_uti += round(batch[1]/self.cart_capacity, 2)
                    cart_uti = round(cart_uti, 2)
                    f_count += 1
                self.cart_utility += round(cart_uti/len(collected_batches), 2)
                self.file_count += f_count

                # Processing routing variation
                self.routing.run(collected_batches)
                calculated_compl_time = self.routing.count_completion_time()
                
                # Counting total completion time
                for idx, compl_time in enumerate(calculated_compl_time):
                    # continue process if not limit exceeded
                    if (not self.limit_exceeded):                        
                        if (len(self.picker_list) == self.picker):
                            # If all picker busy, finish time added to the soonest picker
                            finTime = self.picker_list[0] + compl_time + timedelta(minutes=1)
                        else:
                            # else assign new picker
                            finTime = time_now + compl_time + timedelta(minutes=1)

                        # If the expected finish time more than time limit
                        # Mark as limit exceeded to halt further processing
                        if finTime > (self.initial_time + timedelta(seconds=self.limit)):
                            self.limit_exceeded = True
                        else:
                            self.completion_time += compl_time

                            if (len(self.picker_list) == self.picker):
                                # Update soonest picker finish time
                                self.picker_list[0] = finTime
                            else:
                                # Assign new picker
                                self.picker_list.append(finTime)
                            self.picker_list.sort()
                    
                            # Counting Turn Over Time
                            for order in raw_batch[idx]:
                                tov_time = (finTime - order[0])
                                self.turn_over_time += tov_time
                                if finTime > order[2]:
                                    self.late_count += 1

                # Looping condition
                self.start_row = self.current_row
                
                # Reset back order count
                if self.back_order > 0:
                    self.back_order = 0

                # Update time limit
                self.time_limit += self.delta

            # Advance next order data
            self.current_row += 1

    def check_trigger(self):
        if self.opt == 1:
            return self.fcfs()
        elif self.opt == 2:
            return self.seed()
        elif self.opt == 3:
            return self.max_picker()
        elif self.opt == 4:
            return self.max_cart()
        elif self.opt == 5:
            return self.ug_max_picker()
        elif self.opt == 6:
            return self.ug_max_cart()

    def fcfs(self):
        # FCFS
        if self.env.now >= self.time_limit or self.current_row == self.total_order:
            if self.env.now > self.time_limit:
                self.back_order += 1
            return True
        else:
            return False
    def seed(self):
        # Seed
        if self.current_row >= self.max_order_limit or self.current_row == self.total_order:
            # If current order row exceed order limit go back n row
            # until fits in order limit
            if self.current_row > self.max_order_limit:
                self.back_order += 1
                orderNum = copy.copy(self.current_row)
                while orderNum > self.max_order_limit:
                    orderNum -= 1
                    self.back_order += 1
            return True
        else:
            return False
    def max_picker(self):
        # Max Picker
        return (len(self.picker_list) < self.picker and self.current_row - self.start_row != 0) or self.current_row == self.total_order
    def max_cart(self):
        # Max Cart
        if (self.current_pool[0][1] >= self.cart_capacity and (self.current_row - 1) - self.start_row != 0) or self.current_row == self.total_order:
            # If current pool cache total qty exceed cart capacity limit go back n row
            # until fits in order limit
            if self.current_pool[0][1] > self.cart_capacity:
                self.back_order += 1
                while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                    self.back_order += 1
            return True
        else:
            return False
    def ug_max_picker(self):
        # Urgent First + Max Picker
        # check urgent order
        self.check_urgent()
        return self.urgent_count >= self.max_urgent or (len(self.picker_list) < self.picker and self.current_row - self.start_row != 0) or self.current_row == self.total_order
    def ug_max_cart(self):
        # Urgent First + Max Cart
        # check urgent order
        self.check_urgent()
        if self.urgent_count >= self.max_urgent or (self.current_pool[0][1] >= self.cart_capacity and (self.current_row - 1) - self.start_row != 0) or self.current_row == self.total_order:
            # If current pool cache total qty exceed cart capacity limit go back n row
            # until fits in order limit
            if self.current_pool[0][1] > self.cart_capacity:
                self.back_order += 1
                while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                    self.back_order += 1
            return True
        else:
            return False

    def check_urgent(self):
        # Check urgent order
        # Skip if pool cache still empty
        if self.current_pool[0][1] == 0:
            self.urgent_count = 0
            return 0

        # Reset urgent count
        self.urgent_count = 0
        if self.last_checked_row != self.current_row:
            # Calculate completion time based on
            # current order pool cache
            to_routing = copy.deepcopy(self.current_pool)
            self.routing.run(to_routing)
            compl_time = self.routing.count_completion_time()
            
            # Calculate total completion time
            self.all_compl_time = sum(c.seconds for c in compl_time)
            
            # Update last checked row
            self.last_checked_row = self.current_row
            
            # Update sorted due list
            self.sorted_due_list = sorted(self.order_streams[self.start_row:self.current_row, 2])
        
        # Calculate check time and compare with order due
        check_time = self.initial_time + timedelta(seconds=(self.env.now + self.all_compl_time))
        idx = 0
        while idx < len(self.sorted_due_list) and check_time > self.sorted_due_list[idx]:
            self.urgent_count += 1
            idx += 1
    
    def insort(self, new_due):
        # Insert new element to maintain sorted list
        for i in range(len(self.sorted_due_list)):
            if self.sorted_due_list[i] > new_due:
                index = i
                break
        self.sorted_due_list = self.sorted_due_list[: i] + [new_due] + self.sorted_due_list[i :]
