import math
import os
import re
from datetime import timedelta

import numpy as np
import pandas as pd
import statistics


def reading_file():
    file_list = os.listdir('orderFile')
    fname = []

    for file in file_list:
        try:
            fn = pd.read_csv('orderFile/' + file, index_col=0)
        except:
            print('File cannot be opened!')
            exit()

        fn['Created Time'] = pd.to_datetime(fn['Created Time'])
        created_time = fn['Created Time'].tolist()
        due_time = pd.to_datetime(fn['Due Time']).tolist()
        due_time = [dt + timedelta(days=int(dt <= ct)) for dt, ct in zip(due_time, created_time)]
        fn['Due Time'] = due_time

        pos = fn['Order List'].tolist()
        mismatch_qty_idx = [i for i, p in enumerate(pos) if sum(int(qty) for _, qty in string_to_list(p)) != fn['Total Item'][i]]
        pos = [sort_position(collect_position(string_to_list(p))) for i, p in enumerate(pos) if i not in mismatch_qty_idx]
        fn = fn.drop(index=fn.index[mismatch_qty_idx])
        fn['Position'] = pos
        fn = fn.drop('Order List', axis=1)

        fname.append(fn)

    return fname


def string_to_list(string):
    return [[int(qty) for qty in re.findall('[0-9]+', pos)] for pos in re.findall('\(.+?\)', string)]


def position(pos):
    pos1 = (int(pos) - 1) // 32 + 1
    pos2a = math.ceil(((int(pos) % 32) / 2))
    pos2 = 16 if pos2a == 0 else pos2a
    return [pos1, [pos2]]


def collect_position(filelist):
    return sorted([position(pos[0]) for pos in filelist])


def sort_position(filelist):
    aisle_dict = {}
    
    for pos in filelist:
        aisle_num = pos[0]
        if aisle_num not in aisle_dict:
            aisle_dict[aisle_num] = set(pos[1])
        else:
            aisle_dict[aisle_num].update(pos[1])

    sorted_aisles = sorted(aisle_dict.items())
    newlist = []

    for aisle_num, positions in sorted_aisles:
        newlist.append([aisle_num, sorted(positions)])

    return newlist


def count_average(filelist):
    return round(statistics.mean(filelist), 2)


def count_total_order(filelist):
    return len([fn['Total Item'].to_list() for fn in filelist])


def count_total_item(filelist):
    return sum([sum(fn['Total Item'].to_list()) for fn in filelist])


def average_tct(totalCompletionTime):
    if not totalCompletionTime:
        return [timedelta(seconds=0)]

    sumTct = sum(totalCompletionTime, timedelta(seconds=0))
    lenTct = len(totalCompletionTime)
    aveTct = sumTct / lenTct

    totalCompletionTime.append(aveTct)
    return totalCompletionTime
