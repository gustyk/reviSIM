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
                distance = (position[-1][0]-1)*2 + position[-1][1][-1]*2 - 1
            else:
                distance = position[-1][0]-1 + 8
                a = 0
                while a < (len(position))-1:
                    dt = position[a][1]
                    dt.insert(0, 0)
                    dt.append(16)
                    gap = []
                    b = 1
                    while b < len(dt):
                        gp = dt[b] - dt[(b-1)]
                        gap.append(gp)
                        b += 1
                    gap.sort()
                    gap.pop()
                    distance += sum(gap)
                    a += 1
                distance *= 2
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

    # S-shape routing policies distance list
    def s_shape_distance_list(self):
        for fn in self.filelist:
            position = fn['Position'].to_list()
            order = fn['Total Item'].to_list()
            distance = list()
            for post in position:
                if (len(post)%2) != 0:
                    dis = (post[-1][0]-1)*4 + (len(post)-1)*16 + post[-1][1][-1]*2 - 1
                else:
                    dis = (post[-1][0]-1)*4 + len(post)*16
                distance.append(dis)
            completionTime = list()
            a = 0
            while a < len(distance):
                compTime = timedelta(seconds = (distance[a] + order[a]*3))
                completionTime.append(compTime)
                a += 1
            fn['Completion Time'] = completionTime
        return self.filelist

    # Largest gap routing policies distance list
    def largest_gap_distance_list(self):
        for fn in self.filelist:
            position = fn['Position'].to_list()
            order = fn['Total Item'].to_list()
            distance = list()
            for post in position:
                if len(post) == 1:
                    dis = (post[-1][0]-1)*2 + post[-1][1][-1]*2 - 1
                else:
                    dis = post[-1][0]-1 + 8
                    a = 0
                    while a < (len(post))-1:
                        dt = post[a][1]
                        dt.insert(0, 0)
                        dt.append(16)
                        gap = []
                        b = 1
                        while b < len(dt):
                            gp = dt[b] - dt[(b-1)]
                            gap.append(gp)
                            b += 1
                        gap.sort()
                        gap.pop()
                        dis += sum(gap)
                        a += 1
                    dis *= 2
                distance.append(dis)
            completionTime = list()
            a = 0
            while a < len(distance):
                compTime = timedelta(seconds = (distance[a] * order[a]))
                completionTime.append(compTime)
                a += 1
            fn['Completion Time'] = completionTime
        return self.filelist
