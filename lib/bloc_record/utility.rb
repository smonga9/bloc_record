module BlocRecord
  module Utility
    #1
    extend self

    def underscore(camel_cased_word)
 #      #2
        string = camel_cased_word.gsub(/::/, '/')
 #      #3
        string.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
 #      #4
        string.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
 #      #5
        string.tr!("-", "_")
 #      #6
        string.downcase
     end

     def sql_strings(value)
       case value
       when String
         "'#{value}'"
       when Numeric
         value.to_s
       else
         "null"
       end
     end

     def convert_keys(options)
       options.keys.each {|k| options[k.to_s] = options.delete(k) if k.kind_of?(Symbol)}
       options
     end

     def instance_variables_to_hash(obj)
       Hash[obj.instance_variables.map{ |var| ["#{var.to_s.delete('@')}", obj.instance_variable_get(var.to_s)]}]
     end

     def reload_obj(dirty_obj)
       persisted_obj = dirty_obj.class.find_one(dirty_obj.id)
       dirty_obj.instance_variables.each do |instance_variable|
         dirty_obj.instance_variable_set(instance_variable, persisted_obj.instance_variable_get(instance_variable))
       end
     end

     def valid_ids?(ids)
     			ids.each do |id|
     				if not is_pos_int?(id)
     					return false
     				end
     			end
     			true
     		end

     		def is_pos_int?(id)
     			if not id.is_a? Integer
     				return false
     			elsif id < 1
     				return false
     			end
     			true
     		end

     		def map(&block)
     			result = []
     			each do |element|
     				result << block.call(element)
     			end
     			result
     		end

   end
 end
