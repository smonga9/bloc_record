require 'sqlite3'

 module Selection
   def find(*ids)
     if ids.length == 1
       find_one(ids.first)

     elsif ids.length > 1
    if BlocRecord::Utility.valid_ids?(ids)
      sql = <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
      SQL
      puts sql
      rows = connection.execute sql
      rows_to_array(rows)
    else
      puts "Invalid IDs #{ids}"
    end
  end
 end

  def find_one(id)
    if BlocRecord::Utility.is_pos_int?(id)
			sql = <<-SQL
				SELECT #{columns.join ","} FROM #{table}
				WHERE id = #{id};
			SQL
			puts sql
			row = connection.get_first_row sql
			init_object_from_row(row)
		end
   end

   def find_by(attribute, value)
     rows = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
        SQL
        rows_to_array(rows)
   end

   def take(num=1)
     if BlocRecord::Utility.is_pos_int?(num)
       if num > 1
         sql = <<-SQL
           SELECT #{columns.join ","} FROM #{table}
           ORDER BY random()
           LIMIT #{num};
               SQL
               puts sql
               rows = connection.execute sql
               rows_to_array(rows)
             else
               take_one
             end
           else
             puts "Invalid arg: #{num}"
       end
   end

   def take_one
        row = connection.get_first_row <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY random()
          LIMIT 1;
        SQL
        init_object_from_row(row)
   end
   def first
        row = connection.get_first_row <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY id ASC LIMIT 1;
        SQL
        init_object_from_row(row)
   end
      def last
        row = connection.get_first_row <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY id DESC LIMIT 1;
        SQL
        init_object_from_row(row)
      end
      def all
           rows = connection.execute <<-SQL
             SELECT #{columns.join ","} FROM #{table};
           SQL
           rows_to_array(rows)
      end

   private

   def find_each(options = {}, &block)
   		# offset = options.has_key?(:start) ? "OFFSET #{options[:start]}" : ""
   		# limit = options.has_key?(:batch_size) ? "LIMIT #{options[:batch_size]}" : "LIMIT #{count}"
   		# sql = <<-SQL
   		# 	SELECT #{columns.join ","} FROM #{table}
   		# 	ORDER BY id
   		# 	#{limit} #{offset};
   		# SQL
   		# puts sql
   		# rows = connection.execute sql
   		# rows = rows_to_array(rows)
   		# yield rows if block_given?
   		if block_given?
   			find_in_batches(options) do | records, batch |
   				records.each do | record |
   					yield record
   				end
   				break
   			end
   		end
   	end

    def order(*args)
    		order_array = []
    		args.each do |arg|
    			case arg
    			when String
    				order_array.push(arg)
    			when Symbol
    				order_array.push(arg.to_s)
    			when Hash
    				order_array << arg.map{|key, value| "#{key} #{value}"}
    			end
    		end
    		order_command = order_array.join(",")

    		rows = connection.execute <<-SQL
    			SELECT * FROM #{table}
          ORDER BY #{order_command};
		 SQL
		 rows_to_array(rows)

	 end

   def join(*args)
   		if args.count > 1
   			joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
   			rows = connection.execute <<-SQL
   				SELECT * FROM #{table} #{joins};
   			SQL
   		else
   			case args.first
        when String
      rows = connection.execute <<-SQL
        SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
      SQL
    when Symbol
      rows = connection.execute <<-SQL
        SELECT * FROM #{table}
        INNER JOIN #{args.first} ON #{arg.first}.#{table}_id = #{table}.id;
      SQL
    when Hash
      #extract the options from the hash
      second_table = args[0].keys.first
      third_table = args[0].keys.first
      rows = connection.execute <<-SQL
        SELECT * FROM #{table}
        INNER JOIN #{second_table} ON #{second_table}.#{table}_id = #{table}.id
				INNER JOIN #{third_table} ON #{third_table}.#{second_table}_id = #{second_table}.id;
				SQL

			 end
		 end
		 rows_to_array(rows)
	 end

   def find_each(options={})
   		start = options[:start]
   		batch_size = options[:batch_size]
   		# check options for start and batch_size values
   		if start != nil && batch_size != nil
   			rows = connection.execute <<-SQL
   				SELECT #{columns.join ","} FROM #{table}
   				LIMIT #{batch_size} OFFSET #{start};
   			SQL
   		elsif start != nil && batch_size == nil
   			rows = connection.execute <<-SQL
   				SELECT #{columns.join ","} FROM #{table}
   				OFFSET #{start};
   			SQL
   		elsif start == nil && batch_size != nil
   			rows = connection.execute <<-SQL
   				SELECT #{columns.join ","} FROM #{table}
   				LIMIT #{batch_size};
   			SQL
   		else
   			rows = connection.execute <<-SQL
   				SELECT #{columns.join ","} FROM #{table};
   			SQL
   		end
   		rows.each do |row|
   			yield init_object_from_row(row)
   		end
   	end


   	def find_in_batches(options={}, &block)
   		start = options.has_key?(:start) ? options[:start] : 0
   		batch_size = options.has_key?(:batch_size) ? options[:batch_size] : 100
   		batch = 1
   		while start < count
   			sql = <<-SQL
   				SELECT #{columns.join ","} FROM #{table}
   				ORDER BY id
   				LIMIT #{batch_size} OFFSET #{start};
   			SQL
   			puts sql
   			rows = connection.execute sql
   			rows = rows_to_array(rows)

   			yield rows, batch if block_given?

   			start += batch_size
   			batch += 1
   		end
   	end

   def init_object_from_row(row)
     if row
       data = Hash[columns.zip(row)]
       new(data)
     end
   end
   def rows_to_array(rows)
     collection = BlocRecord::Collection.new
     rows.each { |row| collection << new(Hash[columns.zip(row)]) }
     collection
   end
 end
