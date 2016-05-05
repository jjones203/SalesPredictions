import csv

fieldnames = ['date', 'store_nbr', 'item_nbr', 'units']
outfile = open('proc_sales_test.csv', 'w', newline='')
writer = csv.writer(outfile)
zeros = 0
with open('train.csv') as csvfile:
    reader = csv.DictReader(csvfile, fieldnames)
    # skip headers
    next(reader, None)
    for row in reader:
      if int(row['units']) == 0:
        zeros += 1
        
      date_str = row['date'].split('-')  #'2012-01-02' -> [12, 1, 2]
      date_int = [int(date_str[0])-2000, int(date_str[1]), int(date_str[2])]
      # write [store number, year, month, day, item number, sales]
      row_data = [row['store_nbr'], str(date_int[0]), str(date_int[1]), str(date_int[2]),\
                  row['item_nbr'], row['units']]
      writer.writerow(row_data)
      
outfile.close()
csvfile.close()

print(zeros)
