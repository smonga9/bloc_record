require 'sqlite3'
require 'bloc_record/schema'

module Persistence
  def self.included(base)
     base.extend(ClassMethods)
   end

   def save
        self.save! rescue false
   end

   def save!

     unless self.id
       self.id = self.class.create(BlocRecord::Utility.instance_variables_to_hash(self)).id
       BlocRecord::Utility.reload_obj(self)
       return true
    end

     fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(self.instance_variable_get("@#{col}"))}" }.join(",")

     self.class.connection.execute <<-SQL
       UPDATE #{self.class.table}
       SET #{fields}
       WHERE id = #{self.id};
     SQL

     true
   end

   module ClassMethods
     def create(attrs)
       attrs = BlocRecord::Utility.convert_keys(attrs)
       attrs.delete "id"
       vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }

       connection.execute <<-SQL
         INSERT INTO #{table} (#{attributes.join ","})
         VALUES (#{vals.join ","});
       SQL

       data = Hash[attributes.zip attrs.values]
       data["id"] = connection.execute("SELECT last_insert_rowid();")[0][0]
       new(data)
     end

     def update(ids, updates)
       case updates
			 when Hash
				updates = BlocRecord::Utility.convert_keys(updates)
				updates.delete "id"

				updates_array = updates.map {|key, value| "#{key} = #{BlocRecord::Utility.sql_strings(value)}"}


				if ids.class == Integer
					where_clause = "WHERE id = #{ids};"
				elsif ids.class == Array
					where_clause = ids.empty? ? "," : "WHERE id IN (#{ids.join(",")});"
				else
					where_clause = ";"
				end
				connection.execute <<-SQL
					UPDATE #{table}
					SET #{updates_array.join(",")}
					#{where_clause}
				SQL

				true
			#if multiple records are passed
			when Array
				#cycle through array of updates and update each record
				#respective to its id, which will also be an array of ids
				updates.each_with_index do |data, index|
					update(ids[index], data)
			end
		 end
   end

     def update_all(updates)
       update(nil, updates)
     end

     def method_missing(method, arg)
			#test for a dynamic update method
			if method.match(/update_/)
				#extract the update command and update with the command and value
				method_name = method.to_s.split("update_")[1]
				update_attribute(method_name, arg)
			end
		 end


     def update_attribute(attribute, value)
       self.class.update(self.id, { attribute => value })
     end

     def update_attributes(updates)
       self.class.update(self.id, updates)
     end

   end
end
