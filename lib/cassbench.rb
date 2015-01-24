require 'benchmark/ips'
require 'benchmark/suite'

module CassBench
  class Bench
    @client = nil

    def self.run(&block)
      suite = Benchmark::Suite.create do |suite|

        Benchmark.ips do |bench|
          bench.config time: 5
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
