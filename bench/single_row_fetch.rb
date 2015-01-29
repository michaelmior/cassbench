class SingleRowFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE single_row_fetch " \
                    "(id text PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'};"

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO single_row_fetch (id, data) " \
                             "VALUES (?, ?)"

    # Insert random rows
    1.upto(options[:rows]) do |i|
      session.execute insert, '%010d' % i, data
      self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
        if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
    end
  end

  def self.run(bench, session, options)
    bench.report('single_row_fetch') do |times|
      i = 0
      futures = []
      ids = Array.new(times) { '%010d' % (rand * options[:rows]).ceil }
      while i < times
        query = "SELECT data FROM single_row_fetch WHERE id='#{ids[i]}';"
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
