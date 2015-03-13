import simulation

if __name__ == '__main__':
  print '************************* Running simulation *************************'
  average_latency = simulation.run_simulation('sample_log')
  print average_latency
