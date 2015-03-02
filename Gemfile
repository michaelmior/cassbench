source 'https://rubygems.org'

gem 'benchmark-ips'
gem 'benchmark_suite', git: 'git://github.com/michaelmior/benchmark_suite.git',
                       ref: '5386cad'
gem 'cassandra-driver'
gem 'ffi'
gem 'thor'

group :development do
  gem 'pry'
end

platform :jruby do
  gem 'jmx4r'
end

platform :ruby do
  group :development do
    gem 'pry-byebug'
    gem 'pry-rescue'
    gem 'pry-stack_explorer'
  end
end
