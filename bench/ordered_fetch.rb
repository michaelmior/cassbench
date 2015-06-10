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
    @@indexes = 1.upto(options[:rows]).map { |i| '%010d' % i }
    options[:overwrite].times do
      1.upto(options[:rows]) do |i|
        session.execute insert, @@indexes[i], data
        self.jmx_command cluster, :force_keyspace_flush, options[:keyspace] \
          if options[:flush_every] > 0 && (i % options[:flush_every] == 0)
      end
    end if options[:create]

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
      if options[:random]
        session.execute(@@query, @@indexes.sample(times)).each(&:itself)
      else
        session.execute(@@query, @@indexes[start..(start + times)] + @@indexes.take([0, (start + times) - options[:rows] - 1].max)).each(&:itself)
      end
    else
      0.upto(times - 1) do |i|
        if options[:random]
          session.execute(@@query, @@indexes[(i + start) % options[:rows]]).each(&:itself)
        else
          session.execute(@@query, @@indexes.sample).each(&:itself)
        end
      end
    end
  end

  def self.cleanup(session)
    session.execute "DROP TABLE ordered_fetch;"
  end
end
