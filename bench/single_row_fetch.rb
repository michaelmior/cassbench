CassBench::Bench.run do |bench, session|
  session.execute "CREATE TABLE test (id uuid PRIMARY KEY, data text);"
  session.execute "INSERT INTO test (id, data) VALUES " \
                  "(756716f7-2e54-4715-9f00-91dcbea6cf50, " \
                  "'11111111111111111111111111111111111111111111111111" \
                  "11111111111111111111111111111111111111111111111111');"

  bench.report('fetch') do |times|
    i = 0
    futures = []
    while i < times
      query = "SELECT data FROM test WHERE " \
              "id=756716f7-2e54-4715-9f00-91dcbea6cf50;"
      futures.push session.execute_async(query)
      i += 1
    end

    futures.each(&:get)
  end

  # TODO: Add a cleanup routine
  # session.execute "DROP TABLE test;"
end
