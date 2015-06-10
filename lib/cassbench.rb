# encoding: UTF-8

require 'jmx4r' if RUBY_PLATFORM == 'java'

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
      # Disable auto compaction while running
      jmx_command cluster, :disable_auto_compaction, options[:keyspace]

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

      if options[:repeat] > 0
        # Run all the benchmarks
        data = Hash[benchmarks.map do |benchmark|
          measurements = 1.upto(options[:repeat]).map do
            start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            benchmark.run options[:iterations], session, options
            Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
          end

          [benchmark.name, measurements]
        end]

        # Display all the collected measurements
        data.each do |benchmark, measurements|
          # Simple estimate of margin of error
          # https://en.wikipedia.org/wiki/Margin_of_error#Calculations_assuming_random_sampling
          avg = measurements.inject(0, &:+) * 1.0 / measurements.length
          margin = 0.98 / Math.sqrt(measurements.length) * avg

          puts "#{benchmark.rjust 20}: #{avg.round 2} ± #{margin.round 2}"
        end
      end

      # Run the cleanup for each benchmark
      benchmarks.each { |benchmark| benchmark.cleanup session } \
        if options[:drop]
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
