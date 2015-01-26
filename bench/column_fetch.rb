class ColumnFetch < CassBench::Bench
  def self.setup(session, options)
    session.execute "CREATE TABLE column_fetch (id uuid, col text, " \
                    "data text, PRIMARY KEY (id, col)) " \
                    "WITH caching = '#{options[:caching]}';"

    data = '1' * 100
    insert = session.prepare "INSERT INTO column_fetch (id, col, data) " \
                             "VALUES (?, ?, ?)"
    generator = Cassandra::Uuid::Generator.new

    # Insert 100,000 random rows
    1.upto(100_000) do |i|
      session.execute insert, generator.uuid, '%010d' % i, data
    end
    session.execute insert,
      Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'), '%010d' % 1,
      data
  end

  def self.run(bench, session)
    bench.report('fetch') do |times|
      i = 0
      futures = []
      while i < times
        query = "SELECT data FROM column_fetch WHERE " \
                "id=756716f7-2e54-4715-9f00-91dcbea6cf50 AND col='0';"
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
