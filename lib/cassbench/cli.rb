require 'cassandra'
require 'thor'

module CassBench::CLI
  class CassBenchCLI < Thor
    check_unknown_options!

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
    option :cleanup, type: :boolean, default: true
    option :caching, type: :string, default: 'all',
           enum: ['all', 'keys_only', 'rows_only', 'none']
    option :compression, type: :string, default: 'none',
           enum: ['snappy', 'lz4', 'deflate', 'none']
    option :rows, type: :numeric, default: 100_000
    option :columns, type: :numeric, default: 1
    option :size, type: :numeric, default: 100
    option :random, type: :boolean, default: true
    option :replication_factor, type: :numeric, default: 3
    option :flush_every, type: :numeric, default: 0
    option :overwrite, type: :numeric, default: 1
    def bench(*benchmarks)
      # Initialize a new cluster pointing at the given host
      cluster = Cassandra.cluster hosts: [options[:host]], port: options[:port]
      session = cluster.connect

      # Create the keyspace if asked and select it
      drop_keyspace session, options[:keyspace] if options[:drop]
      create_keyspace session, options
      session.execute "USE #{options[:keyspace]};"

      # Set the class of compressor to use
      x = options.dup
      options = x
      options[:compression] = {
        'snappy'  => 'SnappyCompressor',
        'lz4'     => 'LZ4Compressor',
        'deflate' => 'DeflateCompressor',
        'none'    => ''
      }[options[:compression]]

      # Connect to the cluster and require (execute) the benchmark
      benchmarks.each do |benchmark|
        require_relative "../../bench/#{benchmark}"
      end
      CassBench::Bench.run_all cluster, session, options

      drop_keyspace session, options[:keyspace] if options[:drop]
    end

    private

    # Drop the given keyspace
    def drop_keyspace(session, keyspace)
      begin
        session.execute "DROP KEYSPACE #{keyspace}"
      rescue Cassandra::Errors::ConfigurationError
        # Keyspace doesn't exist, that's ok
      end
    end

    def create_keyspace(session, options)
      return unless options[:create]

      begin
        session.execute "CREATE KEYSPACE #{options[:keyspace]} WITH " \
                        "replication = {'class': 'SimpleStrategy', " \
                        "               'replication_factor': " \
                        "#{options[:replication_factor]}};"
      rescue Cassandra::Errors::AlreadyExistsError
        # Keyspace already exists, that's ok
      end
    end
  end
end
