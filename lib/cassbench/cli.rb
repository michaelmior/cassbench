require 'cassandra'
require 'thor'

module CassBench::CLI
  class CassBenchCLI < Thor
    desc 'run BENCHMARK',
         'runs the BENCHMARK and reports results'
    option :host, type: :string, default: 'localhost'
    option :port, type: :numeric, default: 9042
    def bench(benchmark)
      cluster = Cassandra.cluster hosts: [options[:host]], port: options[:port]
      client = cluster.connect
    end
  end
end
