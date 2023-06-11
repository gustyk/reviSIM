import sys
sys.path.append('d:\coding\copilot\wh_opt\modules')
import general_function as gf
import numpy as np
from datetime import timedelta

class batchings:
    def __init__(self, opt, cart_capacity, routing, urgent_threshold):
        self.opt = opt
        self.cart_capacity = cart_capacity
        self.routing = routing
        self.urgent_threshold = urgent_threshold
    
    def run(self, file, current_time=None):
        self.file = file
        self.current_time = current_time
        if self.opt == 1:
            return self.fcfs()
        elif self.opt == 2:
            return self.seed_due_late()

    def fcfs(self):
        # fcfs
        rfile = list()
        qt = self.file[:,1]
        # qt = self.file['Total Item'].to_list()
        a = 0
        b = 0
        total = 0
        i = 0
        while i < len(qt):
            total += qt[i]
            if total > self.cart_capacity:
                fl = self.file[a:b]
                a = b
                total = qt[i]
                if len(fl) > 0:
                    rfile.append(fl)
            b += 1
            i += 1
        fl = self.file[a:]
        if len(fl) > 0:
            rfile.append(fl)
        return rfile
 
    def seed(self):
        # seed
        qt = self.file[:,1]
        position = self.file[:,3]
        aisle_position = list()
        for post in position:
            aisle = list()
            for pos in post:
                aisle.append(pos[0])
            aisle_position.append(aisle)
        total = 0
        index_start = list(range(0, len(qt)))
        index_list = list()
        in_ob = list()
        ob = list()
        while len(index_start) != 0:
            # Setting OB if OB is empty
            if len(ob) == 0:
                ob = aisle_position[index_start[0]]
                ins = index_start[0]
                for in_start in index_start:
                    if len(ob) > len(aisle_position[in_start]):
                        ob = aisle_position[in_start]
                        ins = in_start
                total += qt[ins]
                index_start.remove(ins)
                in_ob.append(ins)
            # Checking possible OPY
            if len(index_start) == 0:
                index_list.append(in_ob)
            else:
                ob_list = list()
                for ind in index_start:
                    if (total + qt[ind]) <= self.cart_capacity:
                        ob_list.append(ind)
                # If there's no possible OPY
                if len(ob_list) == 0:
                    index_list.append(in_ob)
                    total = 0
                    in_ob = list()
                    ob = list()
                # If there's possible OPY
                else:
                    if len(ob_list) == 1:
                        in_samad = ob_list[0]
                    else: 
                        sad_tb = list()
                        for obl in ob_list:
                            sad = list()
                            for r in aisle_position[obl]:
                                tb = list()
                                for b in ob:
                                    tb.append(abs(r-b))
                                tb.sort()
                                sad.append(tb[0])
                            sad_tb.append(sad)
                        sad_tr = list()
                        for obl in ob_list:
                            sad = list()
                            for b in ob:
                                tr = list()
                                for r in aisle_position[obl]:
                                    tr.append(abs(b-r))
                                tr.sort()
                                sad.append(tr[0])
                            sad_tr.append(sad)
                        amad = list()
                        i = 0
                        while i < len(sad_tb):
                            anad_rb = sum(sad_tb[i])/len(sad_tb[i])
                            anad_br = sum(sad_tr[i])/len(sad_tr[i])
                            amad.append((anad_rb + anad_br) / 2)
                            i += 1
                        samad = amad[0]
                        in_samad = ob_list[0]
                        i = 1
                        while i < len(amad):
                            if samad > amad[i]:
                                samad = amad[i]
                                in_samad = ob_list[i]
                            i += 1
                    in_ob.append(in_samad)
                    total += qt[in_samad]
                    ob += aisle_position[in_samad]
                    index_start.remove(in_samad)
                    if len(index_start) == 0:
                        index_list.append(in_ob)
        rfile = list()
        for il in index_list:
            fl = self.file[il]
            rfile.append(fl)
        return rfile
    
    def seed_due(self):
        # seed
        # sort by due ascending
        self.file = self.file[self.file[:, 2].argsort()]
        qt = self.file[:,1]
        position = self.file[:,3]
        aisle_position = list()
        for post in position:
            aisle = list()
            for pos in post:
                aisle.append(pos[0])
            aisle_position.append(aisle)
        total = 0
        index_start = list(range(0, len(qt)))
        index_list = list()
        in_ob = list()
        ob = list()
        while len(index_start) != 0:
            # Setting OB if OB is empty
            if len(ob) == 0:
                ob = aisle_position[index_start[0]]
                ins = index_start[0]
                total += qt[ins]
                index_start.remove(ins)
                in_ob.append(ins)
            # Checking possible OPY
            if len(index_start) == 0:
                index_list.append(in_ob)
            else:
                ob_list = list()
                for ind in index_start:
                    if (total + qt[ind]) <= self.cart_capacity:
                        ob_list.append(ind)
                # If there's no possible OPY
                if len(ob_list) == 0:
                    index_list.append(in_ob)
                    total = 0
                    in_ob = list()
                    ob = list()
                # If there's possible OPY
                else:
                    if len(ob_list) == 1:
                        in_samad = ob_list[0]
                    else: 
                        sad_tb = list()
                        for obl in ob_list:
                            sad = list()
                            for r in aisle_position[obl]:
                                tb = list()
                                for b in ob:
                                    tb.append(abs(r-b))
                                tb.sort()
                                sad.append(tb[0])
                            sad_tb.append(sad)
                        sad_tr = list()
                        for obl in ob_list:
                            sad = list()
                            for b in ob:
                                tr = list()
                                for r in aisle_position[obl]:
                                    tr.append(abs(b-r))
                                tr.sort()
                                sad.append(tr[0])
                            sad_tr.append(sad)
                        amad = list()
                        i = 0
                        while i < len(sad_tb):
                            anad_rb = sum(sad_tb[i])/len(sad_tb[i])
                            anad_br = sum(sad_tr[i])/len(sad_tr[i])
                            amad.append((anad_rb + anad_br) / 2)
                            i += 1
                        samad = amad[0]
                        in_samad = ob_list[0]
                        i = 1
                        while i < len(amad):
                            if samad > amad[i]:
                                samad = amad[i]
                                in_samad = ob_list[i]
                            i += 1
                    in_ob.append(in_samad)
                    total += qt[in_samad]
                    ob += aisle_position[in_samad]
                    index_start.remove(in_samad)
                    if len(index_start) == 0:
                        index_list.append(in_ob)
        rfile = list()
        for il in index_list:
            fl = self.file[il]
            rfile.append(fl)
        return rfile
    
    def seed_due_late(self):
        # seed
        # sort by due ascending
        self.file = self.file[self.file[:, 2].argsort()]
        rfile = list()
        a = 0
        b = 0
        total = 0
        i = 0
        tempfile = list()
        while i < len(self.file):
            total += self.file[i][1] - 1
            late = False
            
            tempfile = self.file[a:b]
            if len(tempfile) > 0:
                to_routing = self.collect_batch([tempfile])
                self.routing.run(to_routing)
                compl_time = self.routing.count_completion_time()
                all_compl_time = sum(c.total_seconds() for c in compl_time)
                check_time = self.current_time + timedelta(seconds=all_compl_time+self.urgent_threshold)
                if check_time >= self.file[i-1][2]:
                    late = True

            if total > self.cart_capacity or late:
                fl = self.file[a:b]
                a = b
                total = self.file[i][1]
                tempfile = list()
                if len(fl) > 0:
                    rfile.append(fl)
            
            b += 1
            i += 1
        fl = self.file[a:]
        if len(fl) > 0:
            rfile.append(fl)
        return rfile

    def collect_batch(self, filelist):
        # Collect position and total item from batching
        rfile = list()
        for fl in filelist:
            pair = []
            loc = fl[:,3]
            location = []
            for lc in loc:
                location += lc
            location = gf.sort_position(location)
            pair.append(location)
            quan = fl[:,1]
            quantity = 0
            for qt in quan:
                quantity += qt
            pair.append(quantity)
            rfile.append(pair)
        return rfile