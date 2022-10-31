from datetime import timedelta
from modules import general_function as gf

import copy

class routings:
    def __init__(self, opt):
        self.opt = opt
        
    def run(self, filelist):
        self.filelist = copy.deepcopy(filelist)
        if self.opt == 1:
            return self.s_shape()
        elif self.opt == 2:
            return self.largest_gap()

    def test(self):
        # Reading order file
        fname = gf.reading_file()
        compl_time = list()
        for fn in fname:
            np_fn = fn.to_numpy()
            self.filelist = [[[], 0]]
            for idx, order in enumerate(np_fn):
                for position in order[3]:
                    self.filelist[0][0].append(position)
                self.filelist[0][1] += order[1]
            if self.opt == 1:
                self.s_shape()
            elif self.opt == 2:
                self.largest_gap()
            
            compl_time.append(self.count_completion_time())
        return compl_time
            
    def s_shape(self):
        # s-shape
        for file in self.filelist:
            position = file[0]
            if (len(position)%2) != 0:
                distance = (position[-1][0]-1)*4 + (len(position)-1)*16 + position[-1][1][-1]*2 - 1
            else:
                distance = (position[-1][0]-1)*4 + len(position)*16
            file[0] = distance
        return self.filelist
    
    def largest_gap(self):
        # Largest Gap
        for file in self.filelist:
            position = file[0]
            if len(position) == 1:
                distance = (position[-1][0]-1)*4 + position[-1][1][-1]*2 - 1
            elif len(position) == 2:
                distance = (position[-1][0]-1)*4 + 32
            else:
                distance = (position[-1][0]-1) * 4 + 32
                a = 1
                while a < (len(position))-1:
                    dt = position[a][1]
                    dt.insert(0, 0)
                    dt.append(17)
                    gap = []
                    b = 1
                    while b < len(dt):
                        gp = dt[b] - dt[(b-1)]
                        gap.append(gp)
                        b += 1
                    gap.sort()
                    gap.pop()
                    totgap = sum(gap)
                    distance = distance + (totgap*2) - 1
                    a += 1
            file[0] = distance
        return self.filelist

    def count_completion_time(self):
        # Counting completion time
        a = 0
        while a < len(self.filelist):
            comptime = self.filelist[a][0] + self.filelist[a][1] * 3
            ctime = timedelta(seconds = comptime)
            self.filelist[a] = ctime
            a += 1
        return self.filelist
