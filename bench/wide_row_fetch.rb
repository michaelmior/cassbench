class WideRowFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE wide_row_fetch (id text, col text, " \
                    "data text, PRIMARY KEY (id, col)) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : " \
                    "            '#{options[:compaction_strategy]}', " \
                    "            'enabled': false };" if options[:create]

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO wide_row_fetch (id, col, data) " \
                             "VALUES (?, ?, ?)"

    # Insert random rows
    @@indexes = 1.upto([options[:rows], options[:columns]].max).map do
      |i| '%010d' % i
    end
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        1.upto(options[:columns]) do |j|
          session.execute insert, @@indexes[i - 1],
                                  @@indexes[j - 1], data
        end
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end if options[:create]

    if options[:batch]
      @@query = "SELECT data FROM wide_row_fetch WHERE id IN ?;"
    else
      @@query = "SELECT data FROM wide_row_fetch WHERE id=?;"
    end
  end

  def self.run(times, session, options)
    # Start at a random offset
    start = rand options[:rows]

    if options[:batch]
      if options[:random]
        session.execute(@@query, @@indexes.sample(times)).each(&:itself)
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE wide_row_fetch;"
  end
end
