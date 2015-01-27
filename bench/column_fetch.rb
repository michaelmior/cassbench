class ColumnFetch < CassBench::Bench
  @@max_rows = 100_000

  def self.setup(session, options)
    session.execute "CREATE TABLE column_fetch (id text, col text, " \
                    "data text, PRIMARY KEY (id, col)) " \
                    "WITH caching = '#{options[:caching]}';"

    data = '1' * 100
    insert = session.prepare "INSERT INTO column_fetch (id, col, data) " \
                             "VALUES (?, ?, ?)"

    # Insert random rows
    1.upto(@@max_rows) do |i|
      session.execute insert, '%010d' % i, '%010d' % i, data
    end
  end

  def self.run(bench, session)
    bench.report('column_fetch') do |times|
      i = 0
      futures = []
      ids = Array.new(times) { '%010d' % (rand * @@max_rows).ceil }
      while i < times
        query = "SELECT data FROM column_fetch WHERE " \
                "id='#{ids[i]}' AND col='0000000001';"
        futures.push session.execute_async(query)
        i += 1
      end

      futures.each(&:get)
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE column_fetch;"
  end
end
