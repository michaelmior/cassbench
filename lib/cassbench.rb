require 'benchmark/ips'
require 'benchmark/suite'

module Benchmark
  module IPS
    class Job
      # Microseconds per minutes (this forces more iterations)
      MICROSECONDS_PER_100MS = 60_000_000
    end
  end
end

module CassBench
  class Bench
    def self.run_all(session)
      # Find all the loaded benchmarks and run their setup routines
      benchmarks = ObjectSpace.each_object(self.singleton_class).to_a
      benchmarks.select! { |cls| cls != self }
      benchmarks.each { |benchmark| benchmark.setup session }

      # Run all the benchmarks
      suite = Benchmark::Suite.create do |suite|
        Benchmark.ips do |bench|
          bench.config warmup: 30, time: 300
          benchmarks.each { |benchmark| benchmark.run bench, session }
        end
      end

      # Display all the collected reports
      suite.report.each(&:display)

      # Run the cleanup for each benchmark
      benchmarks.each { |benchmark| benchmark.cleanup session }
    end
  end
end
