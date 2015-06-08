class OrderedFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE ordered_fetch " \
                    "(id text PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : " \
                    "            '#{options[:compaction_strategy]}', " \
                    "            'enabled': false };"

    data = '1' * options[:size]
    insert = session.prepare "INSERT INTO ordered_fetch (id, data) " \
                             "VALUES (?, ?)"

    # Insert random rows
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        session.execute insert, '%010d' % i, data
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end

    @@indexes = 0.upto(options[:rows] - 1).map { |n| '%010d' % n }
    @@query = "SELECT data FROM ordered_fetch WHERE id=?;"
  end

  def self.run(bench, session, options)
    bench.report('ordered_fetch') do |times|
      0.upto(times - 1) do |i|
        session.execute @@query, @@indexes[i % options[:rows]]
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE ordered_fetch;"
  end
end
