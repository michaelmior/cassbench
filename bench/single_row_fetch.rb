class SingleRowFetch < CassBench::Bench
  def self.setup(session, options)
    session.execute "CREATE TABLE single_row_fetch " \
                    "(id uuid PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}';"

    data = '1' * 100
    insert = session.prepare "INSERT INTO single_row_fetch (id, data) " \
                             "VALUES (?, ?)"
    generator = Cassandra::Uuid::Generator.new

    # Insert 100,000 random rows
    1.upto(100_000) do
      session.execute insert, generator.uuid, data
    end
    session.execute insert,
      Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
      data
  end

  def self.run(bench, session)
    bench.report('fetch') do |times|
      i = 0
      futures = []
      while i < times
        query = "SELECT data FROM single_row_fetch WHERE " \
                "id=756716f7-2e54-4715-9f00-91dcbea6cf50;"
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
