from pyspark import SparkConf, SparkContext, RDD
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import math

conf = SparkConf().setAppName("Recommender").set("spark.executor.memory", "7g")
conf = SparkConf().setAppName("Recommender").set("spark.storage.memoryFraction", "0.1")
sc = SparkContext(conf=conf)

# get data, make rdd
weather_file = sc.textFile('mini_proc_weather.csv')
weather_data = weather_file.map(lambda l: l.split(','))
# stat_nbr, (year, month, day, avg temp)
weather_data = weather_data.map(lambda l: (int(l[0]), (int(l[1]), int(l[2]), int(l[3]), int(l[4]))))

key_file = sc.textFile('key_store_stat.csv')
key_data = key_file.map(lambda l: l.split(','))
# stat_nbr, store_nbr
key_data = key_data.map(lambda l: (int(l[1]), int(l[0])))
combined_data = key_data.join(weather_data)
store_date_temp = combined_data.map(lambda l: l[1])
# ^ now (store, (YY, MM, DD, avgTemp))
#store_date_temp = store_date_temp.map(lambda l: (str(l[0])+'-'+l[1][0], l[1][1]))

sales_file = sc.textFile('mini_proc_sales.csv')
#[store number, year, month, day, item number, sales]
sales_data = sales_file.map(lambda l: l.split(','))
#[(store #, year, month, day), (item, sales)]
sales_data = sales_data.map(lambda l: ((int(l[0]), int(l[1]), int(l[2]), int(l[3])), (int(l[4]), int(l[5]))))
#[(store #, year, month, day), temp]
store_date_temp = store_date_temp.map(lambda l: ((l[0], l[1][0], l[1][1], l[1][2]), l[1][3]))
sales_temp_data = sales_data.join(store_date_temp)
# ((store, year, month, date), ((item, sales), temp))
ratings_RDD = sales_temp_data.map(lambda l: Rating(l[0][0]*1000+l[1][0][0], l[1][1], l[1][0][1]))
# ((store*1000+item, temp, sales)

#print(ratings_RDD.take(3))

# train model
#training_RDD, validation_RDD = ratings_RDD.randomSplit([8, 2], 0)
#validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
#print(training_RDD.collect().take(3))

seed = 5
iterations = 12
regularization_parameter = 0.1
rank = 4
#errors = [0, 0, 0]
#err = 0
#tolerance = 0.02


training_RDD, test_RDD = ratings_RDD.randomSplit([8, 2], 0)

training_1 = training_RDD.map(lambda l: (l[0], l[1]//5, l[2]))
training_2 = training_RDD.map(lambda l: (l[0], (l[1]+3)//5, l[2]))
training_3 = training_RDD.map(lambda l: (l[0], (l[1]-3)//5, l[2]))

model_1 = ALS.train(training_1, rank, seed=None, iterations=iterations, lambda_=regularization_parameter,\
                    nonnegative = True)
model_2 = ALS.train(training_2, rank, seed=None, iterations=iterations, lambda_=regularization_parameter,\
                    nonnegative = True)
model_3 = ALS.train(training_3, rank, seed=None, iterations=iterations, lambda_=regularization_parameter,\
                    nonnegative = True)

test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1], x[1]//5, (x[1]+3)//5, (x[1]-3)//5))
preds = test_for_predict_RDD.map(lambda x: (x[0], x[1], model_1.predict(x[0], x[2]), model_2.predict(x[0], x[3]),\
                                    model_3.predict(x[0], x[4])))

preds = preds.map(lambda x: ((x[0], x[1]), (x[2][2]+x[3][2]+x[4][2])/3))
print(preds.take(3).collect())
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2])))
#rates_and_preds = rates_and_preds.join(preds)

'''
preds_1 = preds_1.map(lambda x: (x[0], x[1], x[2][2]))
preds_2 = test_for_predict_RDD.map(lambda x: (x[0], x[1], model_2.predict(x[0], x[3])))
preds_2 = preds_2.map(lambda x: (x[0], x[1], x[2][2]))
preds_3 = test_for_predict_RDD.map(lambda x: (x[0], x[1], model_3.predict(x[0], x[4])))
preds_3 = preds_3.map(lambda x: (x[0], x[1], x[2][2]))
preds_all = preds_1.join(preds_2)
preds_all = preds_12.join(preds_3)
preds_all = preds_all.map(lambda x: ((x[0, x[1]]), x[2]))
preds_avg = preds_all.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
preds_avg = preds_avg(lambda x: (x[0], float(x[1][0]/x[1][1])))



    pred_list.append((user[0], user[1], meanp))

predictions = sc.parallelize(pred_list)
#predictions = complete_model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))

rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)


mae = rates_and_preds.map(lambda r: (abs(r[1][0] - r[1][1]))).mean()
rmse = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
logs = rates_and_preds.map(lambda r: (math.log(r[1][1] + 1) - math.log(r[1][0] + 1)))
rmsle = math.sqrt(logs.map(lambda x: x**2).mean())


print("The MAE is {:G}".format(mae))
print("The RMSE is {:G}".format(rmse))
print("The RMSLE is {:G}".format(rmsle))

'''



#print(sales_temp_data.take(3))
#^[((1, 12, 1, 1), ((1, 0), 42)), ((1, 12, 1, 1), ((2, 0), 42)), ((1, 12, 1, 1), ((3, 0), 42))]
