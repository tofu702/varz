import client
import json
import random
import datetime

def benchmark_all_dump(c):
  start_time = datetime.datetime.now()
  result  = c.all_dump()
  end_time = datetime.datetime.now()
  result_size = len(json.dumps(result))
  elapsed_seconds = (end_time - start_time).total_seconds()
  mbps = result_size / elapsed_seconds /(1024*1024)
  print "All dump took: %0.2f, result_size=%d, %0.2f MB/s" % (elapsed_seconds, result_size, mbps)

def benchmark_commands(c, num_commands=262144): #16777216):
  start_time = datetime.datetime.now()
  for x in xrange(num_commands):
    r = random.randrange(2)
    cmd = None
    if r == 0:
      random_counter_command(c)
    elif r == 1:
      random_sampler_command(c)
  end_time = datetime.datetime.now()
  elapsed_seconds = (end_time - start_time).total_seconds()
  print "Commands Took %0.2fs; %0.2f commands per sec" % (elapsed_seconds, num_commands/elapsed_seconds)

def random_variable_name(num_names=2048):
  return "variable_%d" % random.randrange(num_names)

def random_counter_command(c):
  c.counter_increment(random_variable_name())

def random_sampler_command(c, value_range=16384):
  c.sampler_add(random_variable_name(), random.randrange(value_range))


def main():
  c = client.VARZClient()
  c.setup()
  benchmark_commands(c)
  benchmark_all_dump(c)


if __name__ == "__main__":
  main()
