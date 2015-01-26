require 'cassandra'
require 'thor'

module CassBench::CLI
  class CassBenchCLI < Thor
    desc 'run BENCHMARK',
         'runs the BENCHMARK and reports results'
    option :host, type: :string, default: 'localhost'
    option :port, type: :numeric, default: 9042
    option :keyspace, type: :string, default: 'cassbench'
    option :create, type: :boolean, default: false
    option :drop, type: :boolean, default: false
    def bench(benchmark)
      # Initialize a new cluster pointing at the given host
      cluster = Cassandra.cluster hosts: [options[:host]], port: options[:port]
      session = cluster.connect

      # Create the keyspace if asked and select it
      if options[:create]
        begin
          session.execute "CREATE KEYSPACE #{options[:keyspace]} WITH " \
                          "replication = {'class': 'SimpleStrategy', " \
                          "               'replication_factor': 3};"
        rescue Cassandra::Errors::AlreadyExistsError
          # Keyspace already exists, that's ok
        end
      end
      session.execute "USE #{options[:keyspace]};"

      # Connect to the cluster and require (execute) the benchmark
      CassBench::Bench.session = session
      require_relative "../../bench/#{benchmark}"

      session.execute "DROP KEYSPACE #{options[:keyspace]}" if options[:drop]
    end
  end
end
