import datetime
import time

def datetime_to_sec_since_epoch(dt):
  '''Take a python datetime object and convert it to seconds since the unix epoch. Not timezone
     aware'''
  return int(time.mktime(dt.timetuple()))

def sec_since_epoch_to_datetime(sec_since_epoch):
  '''Take some number seconds since the epoch and convert them to a python datetime object. Not
     timezone aware'''
  return datetime.datetime.fromtimestamp(sec_since_epoch)

def epoch_sec_to_minutes_since_epoch(sec_since_epoch):
  '''Return the number of minutes since the epoch (quite a large number)'''
  return (sec_since_epoch / 60)
