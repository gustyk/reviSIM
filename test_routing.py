import modules.routings as routings
import pandas as pd

routing = routings.routings(1)
result = routing.test()

seconds = [o[0].total_seconds() for o in result]

write = pd.DataFrame({
    'CompletionTime': seconds,
    }, columns = ['CompletionTime'])
write.to_csv('result/Test_Routing.csv')
print(result)
