import csv, datetime

fieldnames = ['station_nbr', 'date', 'tmax', 'tmin', 'tavg', 'depart', 'dewpoint',\
'wetbulb', 'heat', 'cool', 'sunrise', 'sunset', 'codesum', 'snowfall', 'preciptotal',\
 'stnpressure', 'sealevel', 'resultspeed', 'resultdir', 'avgspeed']
outfile = open('proc_weather.csv', 'w', newline='')
writer = csv.writer(outfile)
minTemp = 100
maxTemp = -100
with open('weather.csv') as csvfile:
    reader = csv.DictReader(csvfile, fieldnames)
    # skip headers
    next(reader, None)
    for row in reader:
      # 'M' mean temp NA. ~7% of data
      if row['tavg'] != 'M':
        #temp = int(row['tavg'])
        #if temp > maxTemp:
        #  maxTemp = temp
        #if temp < minTemp:
        # minTemp = temp

        date_str = row['date'].split('-')  #'2012-01-02' -> [12, 1, 2]
        date_int = [int(date_str[0])-2000, int(date_str[1]), int(date_str[2])]
        ttuple = datetime.date(date_int[0], date_int[1], date_int[2]).timetuple()
        yday = ttuple[7]
        week = yday//7
        if ttuple[6] > 4:
          weekend = 1
        else:
          weekend = 0
        # write [station number, year, month, day, week, avg temp]
        rowdata = [row['station_nbr'], str(date_int[0]), str(date_int[1]), str(date_int[2]), week, weekend, row['tavg']]
        writer.writerow(rowdata)
          
        #else:
        #  if date_int[0] < 13 or (date_int[0] == 13 and date_int[1] < 4):
        #    na += 1
outfile.close()
csvfile.close()

print(str(maxTemp))
print(str(minTemp))