require 'cassandra'
require 'thor'

module CassBench::CLI
  class CassBenchCLI < Thor
    def self.exit_on_failure?
      true
    end

    desc 'bench BENCHMARKS',
         'runs the BENCHMARKS and reports results'
    option :host, type: :string, default: 'localhost'
    option :port, type: :numeric, default: 9042
    option :keyspace, type: :string, default: 'cassbench'
    option :create, type: :boolean, default: false
    option :drop, type: :boolean, default: false
    option :flush, type: :boolean, default: false
    option :compact, type: :boolean, default: false
    option :caching, type: :string, default: 'all',
           enum: ['all', 'keys_only', 'rows_only', 'none']
    option :rows, type: :numeric, default: 100_000
    option :size, type: :numeric, default: 100
    def bench(*benchmarks)
      # Initialize a new cluster pointing at the given host
      cluster = Cassandra.cluster hosts: [options[:host]], port: options[:port]
      session = cluster.connect

      # Create the keyspace if asked and select it
      if options[:drop]
        begin
          session.execute "DROP KEYSPACE #{options[:keyspace]}"
        rescue Cassandra::Errors::ConfigurationError
          # Keyspace doesn't exist, that's ok
        end
      end
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
      benchmarks.each do |benchmark|
        require_relative "../../bench/#{benchmark}"
      end
      CassBench::Bench.run_all cluster, session, options

      session.execute "DROP KEYSPACE #{options[:keyspace]}" if options[:drop]
    end
  end
end
