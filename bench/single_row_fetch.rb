class SingleRowFetch < CassBench::Bench
  def self.setup(session, options)
    session.execute "CREATE TABLE single_row_fetch " \
                    "(id text PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}';"

    data = '1' * 100
    insert = session.prepare "INSERT INTO single_row_fetch (id, data) " \
                             "VALUES (?, ?)"

    # Insert 100,000 random rows
    1.upto(100_000) do
      session.execute insert, '%010d' % i, data
    end
  end

  def self.run(bench, session)
    bench.report('single_row_fetch') do |times|
      i = 0
      futures = []
      while i < times
        query = "SELECT data FROM single_row_fetch WHERE id='0000000001';"
        futures.push session.execute_async(query)
        i += 1
      end

      futures.each(&:get)
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE single_row_fetch;"
  end
end
