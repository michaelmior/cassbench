class ColumnFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    col_range = 1.upto(options[:columns])
    session.execute "CREATE TABLE cost_test (id text" +
                    (options[:columns] > 0 ? ', ' + col_range.map { |i| "col#{i} text" }.join(', ') : '') +
                    (options[:size] > 0 ? ', data text' : '') + ", PRIMARY KEY (id" +
                    (options[:columns] > 0 ? ', ' + col_range.map { |i| "col#{i}" }.join(', ') : '') +
                    ")) WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : " \
                    "            '#{options[:compaction_strategy]}', " \
                    "            'enabled': false };" if options[:create]

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO cost_test (id" +
                             (options[:columns] > 0 ? ', ' + col_range.map { |i| "col#{i}" }.join(', ') : '') +
                             (options[:size] > 0 ? ', data' : '') +
                             ") VALUES(?" + (options[:size] > 0 ? ', ?' : '') +
                             (options[:columns] > 0 ? ", #{(['?'] * options[:columns]).join ', '}" : '') + ')'

    # Insert random rows
    @@indexes = 1.upto([options[:rows], options[:columns], options[:cardinality]].max).map do |i|
      '%010d' % i
    end
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        1.upto(options[:cardinality]).to_a.repeated_permutation(options[:columns]).each do |cols|
          values = [@@indexes[i-1]] + cols.map { |c| @@indexes[c - 1] }
          values << data if options[:size] > 0
          session.execute insert, *values
        end
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end if options[:create]

    unless options[:batch]
      col_range = 1.upto(options[:filter])
      cols = col_range.map { |i| "col#{i}=?" }.join(' AND ')
      query_str = "SELECT #{options[:all] ? '*' : 'data'} FROM cost_test WHERE id = ?"
      query_str += " AND #{cols}" if options[:filter] > 0
      @@query = session.prepare(query_str + ";")
    end
  end

  def self.run(times, session, options)
    unless options[:batch]
      if options[:random]
        times.times do
          cols = 1.upto(options[:filter]).map do
            @@indexes[0..options[:columns] - 1].sample
          end
          session.execute(@@query, @@indexes[0..options[:rows] -1].sample, *cols).each(&:itself)
        end
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE cost_test;"
  end
end
