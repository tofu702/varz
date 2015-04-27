import client


def main():
  c = client.VARZClient()
  c.setup()
  print "*****Running increments & adds (no output)*****"
  c.counter_increment("test_counter")
  c.sampler_add("test_sampler", 2)
  c.counter_increment("test_counter2")
  print "*****Running all_list*****"
  print c.all_list()
  print "*****Running all_dump*****"
  print c.all_dump()
  print "*****Running all_flush*****"
  print c.all_flush();
  print "*****Running all_dump*****"
  print c.all_dump()

if __name__ == "__main__":
  main()
