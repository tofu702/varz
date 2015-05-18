import datetime
try:
  import simplejson as json
except ImportError:
  import json
import socket

import utils

class VARZClient(object):
  MODE_TCP = 1
  MODE_UDP = 2
  MAX_NAME_LEN = 128

  def __init__(self, hostname='localhost', udp_port=4447, tcp_port=14447):
    self.hostname = hostname
    self.udp_port = udp_port
    self.tcp_port = tcp_port
    self.udp_socket = None

  def setup(self):
    '''Create sockets'''
    self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

  def counter_increment(self, counter_name, amt=1, time=None, mode=None):
    '''Increment a varz counter variable on the remote hosts.
    Arguments
      counter_name: A name for a counter, must be less than 128 characters. Cannot have whitespace.
      amt (optional): The amount to increment the counter by, defaults to 1
      time (optional): The time of the event as a datetime.datetime, defaults to current time. Should
          be utc time.
      mode (optional): Can be used to force either UDP or TCP mode
    Returns: None'''
    counter_name, time, mode = self._defaults_for_name_time_and_mode(counter_name, time, mode)
    sec_since_epoch = utils.datetime_to_sec_since_epoch(time)
    command = "MHTCOUNTERADD %s %d %d;" % (counter_name, sec_since_epoch, amt)
    self._send_with_mode(command, mode)

  def sampler_add(self, sampler_name, value, time=None, mode=None):
    '''Add a sample to the specified sampler. The varz daemon will of course only add the sample
       if its sampling requirements are satisfied. 
    Arguments:
      sampler_name: A name for the sampler, must be less than 128 characters. Cannot have whitespace
      value: The value of the sample
      time (optional): The time of the event as a datetime.datetime, defaults to current time. Should
          be utc time.
      mode (optional): Can be used to force either UDP or TCP mode
    Returns: None'''
    sampler_name, time, mode = self._defaults_for_name_time_and_mode(sampler_name, time, mode)
    sec_since_epoch = utils.datetime_to_sec_since_epoch(time)
    command = "MHTSAMPLEADD %s %d %d;" % (sampler_name, sec_since_epoch, value)
    self._send_with_mode(command, mode)

  def _defaults_for_name_time_and_mode(self, name, time, mode):
    if len(name) > VARZClient.MAX_NAME_LEN:
      raise ValueError("name '%s' is longer than %d", name, VARZClient.MAX_NAME_LEN)
    if not time:
      time = datetime.datetime.now()
    if not mode:
      mode = VARZClient.MODE_UDP # TODO: Let this be an object level setting
    return (name, time, mode)

  def all_dump(self):
    '''Execute the ALLDUMPJSON command, this must be executed over TCP
    Returns: <TODO>'''
    command = "ALLDUMPJSON;"
    return json.loads(self._send_and_receive_tcp_command(command))

  def all_list(self):
    '''Execute the ALLLISTJSON command, this must be executed over TCP
    Returns: {'mhtcounters': [name1, name2...], 'mht_samplers': [name1, name2...]}'''
    command = "ALLLISTJSON;"
    return json.loads(self._send_and_receive_tcp_command(command))

  def all_flush(self):
    '''Execute the ALLFLUSH command, this must be executed over TCP. This command will flush
       everything on the varz server, so be very careful when calling it'''
    command="ALLFLUSH;"
    self._send_and_receive_tcp_command(command)

  def _send_with_mode(self, command_string, mode):
    if mode == VARZClient.MODE_UDP:
      self._send_udp_command(command_string)
    else:
      return self._send_and_receive_tcp_command(command_string)

  def _send_udp_command(self, command_string):
    udp_address = (self.hostname, self.udp_port)
    self.udp_socket.sendto(command_string, udp_address)

  def _send_and_receive_tcp_command(self, command_string): 
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.connect((self.hostname, self.tcp_port))
    try:
      tcp_socket.sendall(command_string)
      recved = ""
      while True:
        new_data = tcp_socket.recv(4096)
        if len(new_data) == 0:
          break
        recved += new_data
      return recved
    finally:
      tcp_socket.close()
