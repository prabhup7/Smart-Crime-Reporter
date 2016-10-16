import logging
logging.basicConfig(level=logging.DEBUG)
from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode
from spyne import Iterable
from urllib2 import urlopen
import urllib2,cookielib
import json
from spyne.decorator import srpc
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication
from datetime import datetime
from collections import OrderedDict
import urllib
import requests
from flask import request
import re
import datetime
import operator


class HelloWorldService(ServiceBase):
    @rpc(str, str, str, _returns=Iterable(Unicode))
    def checkcrime(ctx, lat, lon, radius):
        url = "https://api.spotcrime.com/crimes.json?lat="+str(lat)+"&lon="+str(lon)+"&radius="+str(radius)+"&key=."
        u = requests.get(url)
        source_dict=json.loads(u.text)
        count = 0

#total number of crimes        
        crimes_no = len(source_dict["crimes"])

#crime_type counts
        crime_type = []        
        for key in source_dict["crimes"]:
            crime_type.append(key["type"])

        a=list(set(crime_type))
        crime_type_dict={}

        for i in range(len(a)):
            for j in range(len(crime_type)):
                if crime_type[j] == a[i]:
                    count+=1    
            crime_type_dict[a[i]]=count  
            print a[i] + ":" + str(count)
            count=0

#extract times
        dates_arr1= []
        for key in source_dict["crimes"]:
            date_in = key["date"]
            date_fin = datetime.datetime.strptime(date_in, '%m/%j/%y %I:%M %p').time()
            dates_arr1.append(date_fin)

        slot_12to3 = 0
        slot_3to6 = 0
        slot_6to9 = 0
        slot_9to12 = 0
        slot_12PMto3PM = 0
        slot_3PMto6PM = 0
        slot_6PMto9PM = 0
        slot_9PMto12 = 0

#convert string format time into datetime format
        date_1 = datetime.datetime.strptime('12:00 AM', '%I:%M %p').time()
        date_2 = datetime.datetime.strptime('3:00 AM', '%I:%M %p').time()
        date_3 = datetime.datetime.strptime('6:00 AM', '%I:%M %p').time()
        date_4 = datetime.datetime.strptime('9:00 AM', '%I:%M %p').time()
        date_5 = datetime.datetime.strptime('12:00 PM', '%I:%M %p').time()
        date_6 = datetime.datetime.strptime('3:00 PM', '%I:%M %p').time()
        date_7 = datetime.datetime.strptime('6:00 PM', '%I:%M %p').time()
        date_8 = datetime.datetime.strptime('9:00 PM', '%I:%M %p').time()

#count per time slots
        for date_temp in dates_arr1:
            if (date_temp > date_1) & (date_temp <=date_2):
                slot_12to3+=1
            elif(date_temp > date_2) & (date_temp <=date_3):
                slot_3to6+=1
            elif(date_temp > date_3) & (date_temp <=date_4):
                slot_6to9+=1
            elif(date_temp > date_4) & (date_temp <=date_5):
                slot_9to12+=1
            elif(date_temp > date_5) & (date_temp <=date_6):
                slot_12PMto3PM+=1
            elif(date_temp > date_6) & (date_temp <=date_7):
                slot_3PMto6PM+=1
            elif(date_temp > date_7) & (date_temp <=date_8):
                slot_6PMto9PM+=1
            else:
                slot_9PMto12+=1
        
        slot_count = {"12:01am-3am":slot_12to3,"3:01am-6am":slot_3to6,"6:01am-9am":slot_6to9,
            "9:01am-12noon":slot_9to12,"12:01pm-3pm":slot_12PMto3PM,"3:01pm-6pm":slot_3PMto6PM,
            "6:01pm-9pm":slot_6PMto9PM,"9:01pm-12midnight":slot_9PMto12}
            
        
#extract address
        address_list=[]
        new_address_list=[]
        for key in source_dict["crimes"]:
            addr_1 = key["address"]
            address_list.append(addr_1)

#calculate top 3 addresses
        for x in address_list:
            
            if "OF" in x:
                a=x.split("OF")
                new_address_list.append((a[1]).lstrip())

            elif "BLOCK" in x:
                a=x.split("BLOCK")
                new_address_list.append((a[1]).lstrip())

            elif "&" in x:
                a=x.split("&")
                new_address_list.append((a[0]).lstrip())
                new_address_list.append((a[1]).lstrip())

            elif "BLOCK BLOCK" in x:
                a=x.split("BLOCK BLOCK")
                new_address_list.append((a[1]).lstrip())
            
            else:
                new_address_list.append((x).lstrip())

        list_unique=list(set(new_address_list))
        address_fin={}

        for i in range(len(list_unique)):
            for j in range(len(new_address_list)):
                if new_address_list[j] == list_unique[i]:
                    count+=1       
                    
            address_fin[list_unique[i]]=str(count)
            count=0

        sorted_address_dict = sorted(address_fin.items(), key=operator.itemgetter(1))
        popular_addr = sorted(address_fin, key = address_fin.get, reverse = True)
        string_popular_addr=[]
        for key in popular_addr:
            string_popular_addr.append(str(key))

# print output dictionary 
        out_dict = {'total_crime':crimes_no,
                     'the_most_dangerous_streets':string_popular_addr[:3],
                     'crime_type_account':crime_type_dict,
                     'event_time_count':slot_count
                     }
        yield out_dict

application = Application([HelloWorldService],
    tns='lab2',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)


if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()