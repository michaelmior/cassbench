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
    @client = nil

    def self.run(&block)
      suite = Benchmark::Suite.create do |suite|

        Benchmark.ips do |bench|
          bench.config warmup: 30, time: 300
          block.call bench, @session
        end
      end

      suite.report.each(&:display)
    end

    def self.session=(session)
      @session = session
    end
  end
end
