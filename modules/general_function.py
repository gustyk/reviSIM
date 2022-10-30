# Import libraries
import math
from sys import exit
import os
import re
import numpy as np
import pandas as pd
from datetime import timedelta

# Reading order file
def reading_file():
    file_list = os.listdir('orderFile')
    fname = list()
    for file in file_list:
        try:
            fn = pd.read_csv(
                'orderFile/' + file,
                index_col = 0,
                )
        except:
            print('File cannot be opened!')
            exit()
        fn['Created Time'] = pd.to_datetime(fn['Created Time'])
        createdTime = fn['Created Time'].to_list()
        dueTime = pd.to_datetime(fn['Due Time']).to_list()
        b = 0
        while b < len(dueTime):
            if dueTime[b] <= createdTime[b]:
                dueTime[b] += timedelta(days = 1)
            b += 1
        fn['Due Time'] = dueTime
        pos = fn['Order List'].to_list()
        b = 0
        while b < len(pos):
            pos[b] = string_to_list(pos[b])
            pos[b] = collect_position(pos[b])
            pos[b] = sort_position(pos[b])
            b += 1
        fn['Position'] = pos
        fn.drop('Order List', axis=1, inplace=True)
        fname.append(fn)
    return fname

# Changing string of order to list
def string_to_list(string):
    string = string[1:-1]
    string = re.findall('\(.+?\)', string)
    i = 0
    while i < len(string):
        string[i] = re.findall('[0-9]+', string[i])
        i += 1
    return string

# Convert position
def position(pos):
    pos1 = (pos - 1) // 32 + 1
    pos2a = math.ceil(((pos % 32) / 2))
    if pos2a == 0:
        pos2 = 16
    else:
        pos2 = pos2a
    return [pos1, [pos2]]

# Collecting position from list
def collect_position(filelist):
    newlist = list()
    for pos in filelist:
        newlist.append(position(pos[0]))
    newlist.sort()
    return newlist

# Sorting position
def sort_position(filelist):
    newlist = list()
    aisle = list()
    for num in filelist:
        if not (num[0] in aisle):
            newlist.append(num)
            aisle.append(num[0])
        else:
            i=aisle.index(num[0])
            newlist[i][1].append(num[1][0])
    for num in newlist:
        num[1] = set(num[1])
        num[1] = sorted(num[1])
    newlist.sort()
    return newlist

# Counting average of list
def count_average(filelist):
    avg = round(sum(filelist)/len(filelist), 2)
    filelist.append(avg)
    return filelist

# Counting total order
def count_total_order(filelist):
    totalOrder = list()
    for fn in filelist:
        orderList = fn['Total Item'].to_list()
        totalOrder.append(len(orderList))
    totalOrder = count_average(totalOrder)
    return totalOrder

# Counting total item picked
def count_total_item(filelist):
    totalItemPicked = list()
    for fn in filelist:
        orderList = fn['Total Item'].to_list()
        totalItemPicked.append(sum(orderList))
    totalItemPicked = count_average(totalItemPicked)
    return totalItemPicked

# Counting average of total completion time
def average_tct(totalCompletionTime):
    sumTct = timedelta(seconds=0)
    for tct in totalCompletionTime:
        sumTct += tct
    lenTct = len(totalCompletionTime)
    aveTct = sumTct/lenTct
    totalCompletionTime.append(aveTct)
    return totalCompletionTime