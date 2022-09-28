from modules import general_function as gf
class batchings:
    def __init__(self, opt, cart_capacity):
        self.opt = opt
        self.cart_capacity = cart_capacity
    
    def run(self, file):
        self.file = file
        return getattr(self, 'opt_' + str(self.opt))()

    def opt_1(self):
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
 
    def opt_2(self):
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
            fl = self.file.iloc[il]
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