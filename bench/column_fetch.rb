class ColumnFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE column_fetch (id uuid, col uuid, " \
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
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        1.upto(options[:columns] do |j|
          session.execute insert, Cassandra::Uuid.new(i),
                                  Cassandra::Uuid.new(j), data
        end
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end

    @@indexes = 0.upto(options[:rows] - 1).to_a.shuffle.map do |n|
      Cassandra::Uuid.new(options[:random] ? n : 1)
    end
    @@query = session.prepare "SELECT data FROM column_fetch WHERE id=? " \
                              "AND col=?;"
  end

  def self.run(bench, session, options)
    bench.report('column_fetch') do |times|
      0.upto(times - 1) do |i|
        session.execute @@query, @@indexes[i % options[:rows]], @@indexes.first
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE column_fetch;"
  end
end
