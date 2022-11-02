import pandas as pd
import time
from datetime import timedelta
import copy

class triggers:
    def __init__(self, env, pickers, batching, routing, cart_capacity):
        self.env = env
        self.pickers = pickers
        self.batching = batching
        self.routing = routing
        self.cart_capacity = cart_capacity
        self.cart_utility = 0
        self.completion_time = timedelta(seconds=0)
        self.turn_over_time = timedelta(seconds=0)
        self.total_lateness = timedelta(seconds=0)
        self.tardy_order = 0
        self.start_row = 0
        # start from the second row
        self.current_row = 0
        self.total_item = 0
        self.urgent_status = 0
        self.current_pool = [[[], 0, []]]
        self.num_triggered = 0
        self.processed_item = 0
        self.batch_orders = list()
        self.back_order = 0
        self.all_compl_time = 0
        self.last_checked_row = -1
        self.sorted_due_list = list()
        self.total_batch = 0
        self.processed_order = 0
        self.print_io_station = True
        self.print_ioStation = list()


    def prepare (self, order_streams):
        # Start time if need to track timelapse
        # start_time = time.time()
        
        # Assign order file to process
        self.order_streams = order_streams.to_numpy()
        
        # Assign simulation limit
        # self.limit = limit
        
        # Assign data from order file to list
        self.created_time = self.order_streams[:,0]
        self.total_item = self.order_streams[:,1]
        self.positions = self.order_streams[:,3]
        self.total_order = len(self.created_time)
        
        # Assign Initial Time
        self.initial_time = self.created_time[0].replace(minute=0, second=0)
        self.last_time = self.initial_time
    
    def picking_process(self, compl_time, selected_batch, start_row, current_row):
        self.num_triggered += 1
        with self.pickers.request() as req:
            yield req
            finSeconds = compl_time.total_seconds() + timedelta(minutes=1).seconds
            self.total_batch += 1
            self.completion_time += compl_time
            finTime = self.initial_time + timedelta(seconds=self.env.now+finSeconds)
            self.processed_order += len(selected_batch)
            batch_items = 0
            for order in selected_batch:
                # order[0] is createdTime
                tov_time = (finTime - order[0])
                self.turn_over_time += tov_time
                # order[2] is dueTime
                self.total_lateness += finTime - order[2]
                tots = self.total_lateness.total_seconds()
                if finTime > order[2]:
                    self.tardy_order += 1
                self.processed_item += order[1]
                batch_items += order[1]

            # Counting cart utility
            self.cart_utility += round(batch_items/self.cart_capacity, 2)

            if self.print_io_station:
                self.print_ioStation.append([self.num_triggered, str(start_row) + ':' + str(current_row), self.total_batch, len(selected_batch), self.initial_time + timedelta(seconds=self.env.now), finTime])

            # Advance time
            yield self.env.timeout(finSeconds)

    def batching_routing(self):       
        # Print process detail if required
        # print("%d. Start Row: %d, Current Row: %d, Back Order: %d, secs: %f" % (self.num_triggered, self.start_row, self.current_row, self.back_order, time.time() - start_time))

        # If need to go back one order
        if self.back_order > 0:
            # Shift order batch taking minus back order count
            # if (self.start_row < self.current_row):
            #     batch_orders = self.order_streams[max(0, self.start_row-self.back_order):self.current_row-self.back_order]
            # else:
            batch_orders = self.order_streams[max(0, self.start_row-self.back_order):self.current_row+1-self.back_order]

            # Update order pool cache
            item_qty = self.total_item[self.current_row+1-self.back_order:self.current_row+1]
            self.current_pool[0][0] = self.current_pool[0][0][-self.back_order:]
            self.current_pool[0][1] = sum(item_qty)
            self.current_pool[0][2] = item_qty.tolist()

            trigger_start_row = max(0, self.start_row-self.back_order) + 1
            trigger_end_row = self.current_row+1-self.back_order
        # No need to go back order, take complete batch
        # and empty order pool cache
        else:
            # if (self.start_row < self.current_row and self.current_row != self.total_order - 1):
            #     batch_orders = self.order_streams[self.start_row:self.current_row]
            # else:
            batch_orders = self.order_streams[self.start_row:self.current_row+1]
            self.current_pool = [[[], 0, []]]

            trigger_start_row = self.start_row + 1
            trigger_end_row = self.current_row + 1
        # Looping condition
        self.start_row = self.current_row+1
        
        # Reset back order count
        last_back_order = self.back_order
        self.back_order = 0

        # Processing batching
        raw_batch = self.batching.run(batch_orders)
        ioStation_content = self.batching.collect_batch(raw_batch)

        # Processing routing variation
        self.routing.run(ioStation_content)
        calculated_compl_time = self.routing.count_completion_time()
        
        # Counting total completion time
        for idx, compl_time in enumerate(calculated_compl_time):
            self.env.process(self.picking_process(compl_time, raw_batch[idx], trigger_start_row, trigger_end_row))

    def add_order_to_op(self):
        if (self.current_row < self.total_order):
            # Put current order data to order pool
            self.current_pool[0][0] += self.positions[self.current_row]
            self.current_pool[0][1] += self.total_item[self.current_row]
            self.current_pool[0][2].append(self.total_item[self.current_row])

    def ftwb(self):
        # FTWB
        delta = timedelta(minutes=12).seconds
        time_limit = delta
        while self.current_row < self.total_order:
            yield self.env.timeout(1)

            if self.env.now == time_limit:
                self.batching_routing()
                time_limit += delta

            # Advance next order data
            current_time = self.initial_time + timedelta(seconds=self.env.now)
            if current_time == self.created_time[self.current_row]:
                while self.current_row < self.total_order and current_time == self.created_time[self.current_row]:
                    self.add_order_to_op()
                    self.current_row += 1
        
        self.current_row -= 1

        while self.current_pool[0][1] > 0:
            yield self.env.timeout(1)
            if self.env.now >= time_limit:
                if self.env.now > time_limit:
                    self.back_order += 1
                self.batching_routing()
                time_limit += delta

    def vtwb(self):
        # VTWB
        order_batch = 12
        max_order_batch = order_batch
        while self.current_row < self.total_order:
            yield self.env.timeout((self.created_time[self.current_row] - self.last_time).seconds)
            self.add_order_to_op()
            if self.current_row >= max_order_batch - 1:
                # If current order row exceed order limit go back n row
                # until fits in order limit
                if self.current_row > max_order_batch - 1:
                    self.back_order += 1
                    orderNum = copy.copy(self.current_row)
                    while orderNum > max_order_batch:
                        orderNum -= 1
                        self.back_order += 1
                self.batching_routing()
                max_order_batch += order_batch

            # Advance next order data
            self.last_time = self.created_time[self.current_row]
            self.current_row += 1
                        
    def max_picker(self):
        # Max Picker
        while self.current_row < self.total_order:
            yield self.env.timeout((self.created_time[self.current_row] - self.last_time).seconds)
            self.add_order_to_op()
            if self.pickers.count < self.pickers.capacity and self.current_pool[0][1] > 0:
                self.batching_routing()
            
            # Advance next order data
            self.last_time = self.created_time[self.current_row]
            self.current_row += 1

        self.current_row -= 1
        while self.current_pool[0][1] > 0:
            yield self.env.timeout(1)
            if self.pickers.count < self.pickers.capacity and self.current_pool[0][1] > 0:
                self.batching_routing()

    def max_cart(self):
        # Max Cart
        while self.current_row < self.total_order:
            yield self.env.timeout((self.created_time[self.current_row] - self.last_time).seconds)
            self.add_order_to_op()
            if (self.current_pool[0][1] >= self.cart_capacity):
                # If current pool cache total qty exceed cart capacity limit go back n row
                # until fits in order limit
                if self.current_pool[0][1] > self.cart_capacity:
                    self.back_order += 1
                    while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                        self.back_order += 1
                self.batching_routing()

            # Advance next order data
            self.last_time = self.created_time[self.current_row]
            self.current_row += 1
            
    def ug_max_picker(self):
        # Urgent First + Max Picker
        max_urgent = 1
        while self.current_row < self.total_order:
            yield self.env.timeout((self.created_time[self.current_row] - self.last_time).seconds)
            self.add_order_to_op()
        # check urgent order
            self.check_urgent()
            if self.urgent_status >= max_urgent or (self.pickers.count < self.pickers.capacity and self.current_pool[0][1] > 0):
                self.batching_routing()

            # Advance next order data
            self.last_time = self.created_time[self.current_row]
            self.current_row += 1
            
        self.current_row -= 1
        while self.current_pool[0][1] > 0:
            yield self.env.timeout(1)
            self.last_checked_row = -1
            self.check_urgent()
            if self.urgent_status >= max_urgent or self.pickers.count < self.pickers.capacity and self.current_pool[0][1] > 0:
                self.batching_routing()

    def ug_max_cart(self):
        # Urgent First + Max Cart
        max_urgent = 1
        while self.current_row < self.total_order:
            yield self.env.timeout((self.created_time[self.current_row] - self.last_time).seconds)
            self.add_order_to_op()
            # check urgent order
            self.check_urgent()
            if self.urgent_status >= max_urgent or self.current_pool[0][1] >= self.cart_capacity:
                # If current pool cache total qty exceed cart capacity limit go back n row
                # until fits in order limit
                if self.current_pool[0][1] > self.cart_capacity:
                    self.back_order += 1
                    while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                        self.back_order += 1
                self.batching_routing()

            # Advance next order data
            self.last_time = self.created_time[self.current_row]
            self.current_row += 1

        self.current_row -= 1
        while self.current_pool[0][1] > 0:
            yield self.env.timeout(1)
            self.last_checked_row = -1
            self.check_urgent()
            if self.urgent_status >= max_urgent or self.current_pool[0][1] >= self.cart_capacity:
                if self.current_pool[0][1] > self.cart_capacity:
                    self.back_order += 1
                    while sum(self.current_pool[0][2][:-self.back_order]) > self.cart_capacity:
                        self.back_order += 1
                self.batching_routing()

    def check_urgent(self):
        # Check urgent order
        # Skip if pool cache still empty
        if self.current_pool[0][1] == 0:
            self.urgent_status = 0
            return 0

        # Reset urgent count
        self.urgent_status = 0
        if self.last_checked_row != self.current_row:
            # Calculate completion time based on
            # current order pool cache
            to_routing = copy.deepcopy(self.current_pool)
            self.routing.run(to_routing)
            compl_time = self.routing.count_completion_time()
            
            # Calculate total completion time
            self.all_compl_time = sum(c.total_seconds() for c in compl_time)
            
            # Update last checked row
            self.last_checked_row = self.current_row
            
            # Update sorted due list
            self.sorted_due_list = sorted(self.order_streams[self.start_row:max(1,self.current_row), 2])
        
        # Calculate check time and compare with order due
        check_time = self.initial_time + timedelta(seconds=(self.env.now + self.all_compl_time))
        idx = 0
        while idx < len(self.sorted_due_list) and check_time > self.sorted_due_list[idx]:
            self.urgent_status += 1
            idx += 1    
