require 'benchmark/ips'
require 'benchmark/suite'

require 'jmx4r' if RUBY_PLATFORM == 'java'

module Benchmark
  module IPS
    class Job
      # Microseconds per minutes (this forces more iterations)
      MICROSECONDS_PER_100MS = 60_000_000
    end
  end
end

module SubclassTracking
    def self.extended(superclass)
      (class << superclass; self; end).send :attr_accessor, :subclasses
      (class << superclass; self; end).send :define_method, :inherited do |cls|
        superclass.subclasses << cls
        super(cls)
      end
      superclass.subclasses = []
    end
end

module CassBench
  class Bench
    extend SubclassTracking

    def self.run_all(cluster, session, options)
      # Find all the loaded benchmarks and run their setup routines
      benchmarks = subclasses
      benchmarks.each { |benchmark| benchmark.setup cluster, session, options }

      # Optionally flush or compact the keyspace after setup
      if options[:flush] || options[:compact]
        jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush]
        jmx_command cluster, :force_keyspace_compaction, options[:keyspace] \
          if options[:compact]
      end

      # Run all the benchmarks
      suite = Benchmark::Suite.create do |suite|
        Benchmark.ips do |bench|
          bench.config warmup: 30, time: 300
          benchmarks.each { |benchmark| benchmark.run bench, session, options }
        end
      end

      # Display all the collected reports
      suite.report.each(&:display)

      # Run the cleanup for each benchmark
      benchmarks.each { |benchmark| benchmark.cleanup session } \
        if options[:cleanup]
    end

    # Send a management command to all nodes in a cluster
    def self.jmx_command(cluster, command, keyspace)
      cluster.hosts.each do |host|
        conn = JMX::MBean.create_connection host: host.ip.to_s, port: 7199
        sproxy = JMX::MBean.find_by_name 'org.apache.cassandra.db:'\
                                         'type=StorageService',
                                         connection: conn
        sproxy.public_send command, keyspace, [].to_java(:string)
      end
    end
  end
end
