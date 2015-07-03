class ColumnFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    col_range = 1.upto(options[:columns])
    session.execute "CREATE TABLE column_fetch (id text, " +
                    col_range.map { |i| "col#{i} text" }.join(', ') +
                    ", data text, PRIMARY KEY (id, " +
                    col_range.map { |i| "col#{i}" }.join(', ') +
                    ")) WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : " \
                    "            '#{options[:compaction_strategy]}', " \
                    "            'enabled': false };" if options[:create]

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO column_fetch (id, " +
                             col_range.map { |i| "col#{i}" }.join(', ') +
                             ", data) VALUES (?, ?, " \
                             "#{(['?'] * options[:columns]).join ', '})"

    # Insert random rows
    @@indexes = 1.upto([options[:rows], options[:columns]].max).map do |i|
      '%010d' % i
    end
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        1.upto(options[:columns]) do |j|
          values = [@@indexes[i-1], @@indexes[0]]
          values += 1.upto(options[:columns] - 1).map do |k|
            @@indexes[j - 1]
          end
          values << data
          session.execute insert, *values
        end
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end if options[:create]

    if options[:batch]
      @@query = session.prepare "SELECT data FROM column_fetch " \
                                "WHERE id IN ? AND col1=?;"
    else
      @@query = session.prepare "SELECT data FROM column_fetch WHERE id=? " \
                                "AND col1=?;"
    end
  end

  def self.run(times, session, options)
    if options[:batch]
      if options[:random]
        session.execute(@@query, @@indexes.sample(times),
                        @@indexes.first).each(&:itself)
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE column_fetch;"
  end
end
