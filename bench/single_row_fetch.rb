class SingleRowFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE single_row_fetch " \
                    "(id uuid PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : " \
                    "            '#{options[:compaction_strategy]}', " \
                    "            'enabled': false };" if options[:create]

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO single_row_fetch (id, data) " \
                             "VALUES (?, ?)"

    # Insert random rows
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        session.execute insert, Cassandra::Uuid.new(i), data
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end

    @@indexes = 0.upto(options[:rows] - 1).to_a.shuffle.map do |n|
      Cassandra::Uuid.new(options[:random] ? n : 1)
    end
    @@query = session.prepare "SELECT data FROM single_row_fetch WHERE id=?;"
  end

  def self.run(times, session, options)
    0.upto(times - 1) do |i|
      session.execute @@query, @@indexes[i % options[:rows]]
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE single_row_fetch;"
  end
end
