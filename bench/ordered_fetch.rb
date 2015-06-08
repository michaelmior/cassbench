class OrderedFetch < CassBench::Bench
  def self.setup(cluster, session, options)
    session.execute "CREATE TABLE ordered_fetch " \
                    "(id text PRIMARY KEY, data text) " \
                    "WITH caching = '#{options[:caching]}' AND " \
                    "compression={'sstable_compression': " \
                    "             '#{options[:compression]}'} AND " \
                    "compaction={'class' : " \
                    "            '#{options[:compaction_strategy]}', " \
                    "            'enabled': false };" if options[:create]

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

    @@indexes = 1.upto(options[:rows]).to_a
    @@indexes = @@indexes.shuffle.each_with_index.map do |n, i|
      '%010d' % (options[:random] ? n : i)
    end

    if options[:batch]
      @@query = "SELECT data FROM ordered_fetch WHERE id IN ?;"
    else
      @@query = "SELECT data FROM ordered_fetch WHERE id=?;"
    end
  end

  def self.run(times, session, options)
    # Start at a random offset
    start = rand options[:rows]

    if options[:batch]
      session.execute(@@query, @@indexes.lazy.cycle.drop(start).take(times).to_a).each(&:itself)
    else
      0.upto(times - 1) do |i|
        session.execute(@@query, @@indexes[(i + start) % options[:rows]]).each(&:itself)
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE ordered_fetch;"
  end
end
