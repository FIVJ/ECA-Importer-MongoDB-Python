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
   
   instance_path = "/Users/tassio/NetBeansProjects/ECA-Importer-MongoDB-Python/CSV/Pagamentos"
   
   file_list = [f for f in os.listdir(instance_path)
   if f.startswith('201') and f.endswith('.csv')]
   for filename in sorted(file_list):
         path = os.path.join(instance_path, filename)
         imports=0
         with codecs.open(path,'r', 'ISO-8859-1') as f:
            csv_f = csv.reader(f, delimiter='\t')
            for row in enumerate(csv_f):
               if imports!=0:         
                  state = row[1][0]
                  siafi = row[1][1]
                  city = row[1][2]
            
                  rCity = db.Cities.find({ "Siafi": siafi})
                  if rCity.count() is 0:
                     cities = {
                     "State": state,
                     "Siafi": siafi,
                     "City": city
                     }
                     db.Cities.insert(cities)
                  
                  if imports % 1000 == 0:
                     print("Lines Imports: {} ".format(imports))
                     
               imports+=1   
         path.close()
   
   timer.stop()
   print('\nOption: {}\n'.format(sys.argv[2]))
   print("Total time: " + str( timer.get_time() ) +" s")
   print("Average time: " + str( timer.get_time("average","seg") ) +" s")
   print("Last call: " + str( timer.get_time("last")) +" s")
   print("Stamp 1 of the total: " + str( timer.get_stamp("total","si") ) ) 
   print("Stamp 2 of the total: " + str( timer.get_stamp("total","clock") ) )
   print("\nPattern that ignores zeros:")
   print( timer.get_stamp("total","si",True) )
   print( timer.get_stamp("total","clock",True) )

if __name__ == '__main__':
   main()
