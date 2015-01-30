class ColumnFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE column_fetch (id text, col text, " \
                    "data text, PRIMARY KEY (id, col)) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : 'SizeTieredCompactionStrategy', " \
                    "            'enabled': false };"

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO column_fetch (id, col, data) " \
                             "VALUES (?, ?, ?)"

    # Insert random rows
    1.upto(options[:rows]) do |i|
      session.execute insert, '%010d' % i, '%010d' % i, data
      self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
        if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
    end

    @indexes = 0.upto(options[:rows] - 1).to_a.shuffle.map { |n| '%010d' % n }
    @@query = "SELECT data FROM single_row_fetch WHERE id='?' AND " \
              "col='0000000001;;"
  end

  def self.run(bench, session, options)
    bench.report('column_fetch') do |times|
      0.upto(times - 1) do |i|
        session.execute @@query, @@indexes[i % options[:rows]]
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE column_fetch;"
  end
end
