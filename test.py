import client


def main():
  c = client.VARZClient()
  c.setup()
  c.counter_increment("test_counter")
  c.sampler_add("test_sampler", 2)
  c.counter_increment("test_counter2")
  print c.all_dump()

if __name__ == "__main__":
  main()
