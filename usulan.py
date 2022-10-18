
# jelas
def Routing_sShape(pickingList):
    sesuai method
    return completionTime

# jelas
def Routing_largestGap(pickingList):
    sesuai method 
    return completionTime

# jelas
ioStation_content = []
def Batching_FCFS(orderPool_content, selectedCartCapacity):
    sesuai method
    return batchList
    ioStation_content += batchList

# jelas
ioStation_content = []
def Batching_seed(orderPool_content, selectedCartCapacity):
    sesuai method
    return batchList
    ioStation_content += batchList

time_start = 0
batchTime = 12 minutes
def Trigger_1():
    while time.now < time_start + batchTime:
        orderStream(csv)
        add firstRow to orderPool_content
        env.timeout(delay)
        delete firstRow
    selectedBatching(orderPool_content, selectedCartCapacity)
    empty orderPool_content
    time_start += batchTime

orderBatch = 12
def Trigger_2():
    while OPtotalOrder < orderBatch + 1:
        orderStream(csv)
        add firstRow to orderPool_content
        env.timeout(delay)
        delete firstRow
    selectedBatching(orderPool_content, selectedCartCapacity)
    empty orderPool_content
    OPtotalOrder = 0

def Trigger_3():
    while pickerNumber.count = selectedPickerNumber:
        orderStream(csv)
        add firstRow to orderPool_content
        env.timeout(delay)
        delete firstRow
    selectedBatching(orderPool_content, selectedCartCapacity)
    empty orderPool_content

def Trigger_4():
    while OPtotalItem < selectedCartCapacity:
        orderStream(csv)
        add firstRow to orderPool_content
        env.timeout(delay)
        delete firstRow
    selectedBatching(orderPool_content, selectedCartCapacity)
    empty orderPool_content
    OPtotalItem = 0
    
def Trigger_5():
    while OPurgentStatus = 0 or pickerNumber.count = 0:
        orderStream(csv)
        add firstRow to orderPool_content
        env.timeout(delay)
        delete firstRow
    selectedBatching(orderPool_content, selectedCartCapacity)
    empty orderPool_content
    OPurgentStatus = 0

def Trigger_6():
    while OPurgentStatus = 0 or OPtotalItem < selectedCartCapacity:
        orderStream(csv)
        add firstRow to orderPool_content
        env.timeout(delay)
        delete firstRow
    selectedBatching(orderPool_content, selectedCartCapacity)
    empty orderPool_content
    OPurgentStatus = 0
    OPtotalItem = 0

def readFile(csv):
    read firstRow = orderID, createdTime, orderList, totalItem, dueTime 
    delay = nextRow_createdTime - currentRow_createdTime

urgentStatus = 0
def checkUrgent(dueTime, OPcompletionTime):
    for each order in orderPool_content:
        if env.now + OPcompletionTime > dueTime:
            urgentStatus += 1
        urgentStatus = urgentStatus
        return urgentStatus

OPtotalOrder = 0
OPtotalItem = 0
OPorderList = []
OPcompletionTime = 0
OPurgentStatus = 0
def orderStream(csv):
    readFile(csv)
    OPtotalOrder += 1
    OPtotalItem += totalItem
    OPorderList += [orderList]
    OPcompletionTime = selectedRouting(OPorderList)
    OPurgentStatus = checkUrgent(dueTime, OPcompletionTime)

totalCompletionTime = 0
totalTurnOverTime = 0
totalLateness = 0
totalTardyOrder = 0
totalItemPicked = 0
totalBatches = 0
totalOrderCompleted = 0
pickerNumber = simpy.Resource(env, capacity=selectedPickerNumber)
def pickingProcess(ioStation_content, selectedRouting, pickerNumber):
    while ioStation_content > 0:
        completionTime = selectedRouting(batch in ioStation_content)
        with pickerNumber.request() as req:
            yield req
            yield env.timeout(completionTime)
        totalCompletionTime += completionTime
        for each order in batch:
            turnOverTime = env.now - createdTime
            lateNess = env.now - dueTime --> if lateNess < 0 then tardyOrder += 1
        totalTurnOverTime += turnOverTime 
        totalLateness += lateNess
        totalTardyOrder += tardyOrder
        totalItemPicked += itemPicked
        totalBatches += 1
        totalOrderCompleted += number of order in the batch

def result_generator for each combination:
    OrderFile = selectedOrderFile
    TriggerMethod = selectedTrigger
    BatchingMethod = selectedBatching
    RoutingPolicy = selectedRouting
    NumOfPickers = selectedPickerNumber
    CartCapacity = selectedCartCapacity
    TotalOrder = totalOrderCompleted
    CompletionTime = totalCompletionTime
    AvgCompletionTime = totalCompletionTime / totalOrderCompleted
    TurnOverTime = totalTurnOverTime
    AvgTurnOverTime = totalTurnOverTime / totalOrderCompleted
    TotalItemPicked = totalItemPicked
    AvgPickerUtil = totalCompletionTime / (8 hours * selectedPickerNumber)
    AvgCartUtil = totalItemPicked / (totalBatches * selectedCartCapacity)
    NumOfLate = totalTardyOrder
    TotalLateness = totalLateness
    AvgLateness = totalLateness / totalOrderCompleted
