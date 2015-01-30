class SingleRowFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    @@indexes = 0.upto(options[:rows] - 1).to_a.shuffle.map { |n| '%010d' % n }
    @@query = session.prepare "SELECT data FROM single_row_fetch WHERE id=?;"

    return unless options[:setup]

    session.execute "CREATE TABLE single_row_fetch " \
                    "(id text PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : 'SizeTieredCompactionStrategy', " \
                    "            'enabled': false };"

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO single_row_fetch (id, data) " \
                             "VALUES (?, ?)"

    # Insert random rows
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        session.execute insert, '%010d' % i, data
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end
  end

  def self.run(bench, session, options)
    bench.report('single_row_fetch') do |times|
      0.upto(times - 1) do |i|
        session.execute @@query, @@indexes[i % options[:rows]]
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE single_row_fetch;"
  end
end
