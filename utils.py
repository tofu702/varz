import datetime
import time

def datetime_to_sec_since_epoch(dt):
  '''Take a python datetime object and convert it to seconds since the unix epoch. Not timezone
     aware'''
  return time.mktime(dt.timetuple())

def sec_since_epoch_to_datetime(ms_since_epoch):
  '''Take some number seconds since the epoch and convert them to a python datetime object. Not
     timezone aware'''
  return datetime.datetime.fromtimestamp(ms_since_epoch)

