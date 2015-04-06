import sys

import client
import stats

def print_samplers(samplers):
  print "%16s  ||   %21s   ||   %21s   ||   %21s" % (
      "", "Last Minute", "Last Hour", "All Time"
  )
  print "%16s  ||   %10s %10s   ||   %10s %10s   ||   %10s %10s" % (
      "name", "median", "95th", "median", "95th", "median", "95th"
  )
  print "-" * 110
  for sampler_wrapper in samplers:
    sampler_stats = stats.SamplerStats(sampler_wrapper["value"])
    minute_stats = sampler_stats.last_minute_stats()
    hour_stats = sampler_stats.last_hour_stats()
    all_time_stats = sampler_stats.all_time_stats()
    print "%16s  ||   %10d %10d   ||   %10d %10d   ||   %10d %10d" %  \
        (sampler_wrapper["name"],
         minute_stats["median"], minute_stats["percentile_95"],
         hour_stats["median"], hour_stats["percentile_95"],
         all_time_stats["median"], all_time_stats["percentile_95"])

def print_counters(counters):
  print "%16s  ||  %10s  ||  %10s  ||  %10s" % (
    "name", "last min", "last hr", "all time")
  print "-" * 110
  for counter_wrapper in counters:
    counter_stats = stats.CounterStats(counter_wrapper["value"])
    print "%16s  ||  %10d  ||  %10d  ||  %10d" % \
        (counter_wrapper["name"],
         counter_stats.last_minute_count(),
         counter_stats.last_hour_count(),
         counter_stats.all_time_count())

def sort_vars_by_name(vars_list):
  return sorted(vars_list, key=lambda x:x["name"])

def main(argv):
  c = client.VARZClient()
  dump = c.all_dump()
  print "SAMPLERS"
  print_samplers(sort_vars_by_name(dump["mht_samplers"]))
  print ""
  print "COUNTERS"
  print_counters(sort_vars_by_name(dump["mht_counters"]))

if __name__ == "__main__":
  main(sys.argv)
