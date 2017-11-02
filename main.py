# -*- coding: utf-8 -*-

import pymongo
from pymongo.errors import ConnectionFailure
import CPUtimer
import os
import csv
import codecs

def main():
   timer = CPUtimer.CPUTimer()
   print("Measuring Code Time:\n")
   timer.reset()
   timer.start()
   try:
      db_conn = pymongo.MongoClient('localhost:27017')
      print("Connected to MongoDB successfully!\n")
   except pymongo.errors.ConnectionFailure as e:
      print("Could not connect to server: %s" % e)

   db = db_conn["DB_ECA"]
   
   instance_path = "/Users/tassio/PycharmProjects/ECA-Importer-MongoDB-Python/CSV/Pagamentos"
   
   file_list = [f for f in os.listdir(instance_path)
   if f.startswith('201') and f.endswith('.csv')]
   for filename in sorted(file_list):
       print(filename)
       path = os.path.join(instance_path, filename)
       imports=0
       with codecs.open(path,'r', 'ISO-8859-1') as f:
            csv_f = csv.reader((line.replace('\0','') for line in f), delimiter='\t')
            for row in enumerate(csv_f):
               if imports!=0:         
                  state = row[1][0].upper()
                  siafi = row[1][1]
                  city = row[1][2].upper()
                  nis = row [1][7]
                  beneficiary = row[1][8].upper()
                  action = row[1][6]
                  nfile = filename.upper()
                  function = row[1][3]
                  month = filename[4:6]
                  program = row[1][5]
                  source = row[1][9].upper()
                  subfunction = row[1][4]
                  year = filename[0:4]
                  value = row[1][10]

                  if (state == 'MG' or state == 'RJ' or state == 'SP' or state == 'ES'):
                     region = 'SUDESTE'
                  elif (state == 'RS' or state == 'PR' or state == 'SC'):
                     region = 'SUL'   
                  elif (state == 'GO' or state == 'DF' or state == 'MT' or state == 'MS'):
                     region = 'CENTRO-OESTE'
                  elif (state == 'AC' or state == 'AM' or state == 'RO' or state == 'RR' or state == 'TO'):
                     region = 'NORTE'
                  else:
                     region = 'NORDESTE'

                  rCity = db.Cities.find({"Siafi": siafi})
                  if rCity.count() is 0:
                     cities = {
                        "State": state,
                        "Siafi": siafi,
                        "City": city
                     }
                     db.Cities.insert(cities)
                  
                  rBeneficiaries = db.Beneficiaries.find({ "NIS": nis})
                  if rBeneficiaries.count() is 0:
                     beneficiaries = {
                     "NIS": nis,
                     "Beneficiary": beneficiary
                     }
                     db.Beneficiaries.insert(beneficiaries)
                  
                  payments = {
                  "Action": action,
                  "File": nfile,
                  "Function": function,
                  "Month": month,
                  "NIS": nis,
                  "Beneficiary": beneficiary,
                  "Program": program,
                  "Siafi": siafi,
                  "City": city,
                  "State": state,
                  "Region": region,
                  "Source": source,
                  "SubFunction": subfunction,
                  "Year": year,
                  "Value": value
                  }

                  db.Payments.insert(payments)
                  
                  if imports % 10000 == 0:
                     print("Lines Imports: {} ".format(imports))
                     
               imports+=1
            timer.lap()
   timer.stop()
   print("Total time: " + str(timer.get_time()) +" s")
   print("Average time: " + str(timer.get_time("average","m")) +" min")
   print("Last call: " + str(timer.get_time("last")) +" s")
   print("Stamp 1 of the total: " + str(timer.get_stamp("total","si")))
   print("Stamp 2 of the total: " + str(timer.get_stamp("total","clock")))
   print("\nPattern that ignores zeros:")
   print(timer.get_stamp("total","si",True))
   print(timer.get_stamp("total","clock",True))

if __name__ == '__main__':
   main()
