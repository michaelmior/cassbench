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
      benchmarks.each { |benchmark| benchmark.setup session, options }

      # Optionally flush or compact the keyspace after setup
      if options[:flush] || options[:compact]
        cluster.hosts.each do |host|
          conn = JMX::MBean.create_connection host: host.ip.to_s, port: 7199
          sproxy = JMX::MBean.find_by_name 'org.apache.cassandra.db:'\
                                           'type=StorageService',
                                           connection: conn
          if options[:flush]
            sproxy.force_keyspace_flush options[:keyspace], [].to_java(:string)
          end
          if options[:compact]
            sproxy.force_keyspace_compaction options[:keyspace],
                                             [].to_java(:string)
          end
        end
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
      benchmarks.each { |benchmark| benchmark.cleanup session }
    end
  end
end
