import sys
sys.path.append('d:\coding\copilot\wh_opt\modules')
import general_function as gf
from datetime import timedelta


class Routings:
    def __init__(self, opt):
        self.opt = opt

    def run(self, filelist):
        self.filelist = filelist
        if self.opt == 1:
            return self.s_shape()
        elif self.opt == 2:
            return self.largest_gap()

    def test(self):
        order_files = gf.reading_file()
        completion_times = []
        for order_file in order_files:
            orders = order_file.to_numpy()
            self.filelist = [[[], 0]]
            for order in orders:
                self.filelist[0][0].extend(order[3])
                self.filelist[0][1] += order[1]
            if self.opt == 1:
                self.s_shape()
            elif self.opt == 2:
                self.largest_gap()

            completion_times.append(self.count_completion_time())
        return completion_times

    def s_shape(self):
        for order in self.filelist:
            positions = order[0]
            distance = self.calculate_s_shape_distance(positions)
            order[0] = distance
        return self.filelist

    def largest_gap(self):
        for order in self.filelist:
            positions = order[0]
            distance = self.calculate_largest_gap_distance(positions)
            order[0] = distance
        return self.filelist

    @staticmethod
    def calculate_s_shape_distance(positions):
        if len(positions) % 2 != 0:
            distance = (positions[-1][0] - 1) * 4 + (len(positions) - 1) * 16 + positions[-1][1][-1] * 2 - 1
        else:
            distance = (positions[-1][0] - 1) * 4 + len(positions) * 16
        return distance

    @staticmethod
    def calculate_largest_gap_distance(positions):
        if len(positions) == 1:
            distance = (positions[-1][0] - 1) * 4 + positions[-1][1][-1] * 2 - 1
        elif len(positions) == 2:
            distance = (positions[-1][0] - 1) * 4 + 32
        else:
            distance = (positions[-1][0] - 1) * 4 + 32
            for a in range(1, len(positions) - 1):
                dt = [0] + positions[a][1] + [17]
                gap = [dt[b] - dt[b - 1] for b in range(1, len(dt))]
                gap.sort()
                gap.pop()
                totgap = sum(gap)
                distance += (totgap * 2) - 1
        return distance

    def count_completion_time(self):
        for a in range(len(self.filelist)):
            comptime = self.filelist[a][0] * 1.25 + self.filelist[a][1] * 10 + 180
            self.filelist[a] = timedelta(seconds=comptime)
        return self.filelist
