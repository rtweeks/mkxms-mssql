require 'base64'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  module ExtendedProperties
    def extended_properties
      @extended_properties ||= {}
    end
  end
  
  module Property
    def self.addition_sql(name, value, subject_identification_parts)
      "EXEC sp_addextendedproperty N'%s', %s, %s;" % [
        name,
        value,
        subject_identification_parts.map {|part| "N'#{part}'"}.join(', ')
      ]
    end
    
    module Hosting
      def extended_properties_sql
        self.extended_properties.each_pair.map do |name, value|
          Mkxms::Mssql::Property.addition_sql(name, value, self.property_subject_identifiers)
        end.tap do |v|
          class <<v
            def joined_on_new_lines(indent: '    ')
              map {|i| "\n" + indent + i}.join('')
            end
          end
        end
      end
    end
    
    module SchemaScoped
      def property_subject_identifiers
        ['SCHEMA', Utils::unquoted_name(schema), self.class::SQL_OBJECT_TYPE.upcase, Utils.unquoted_name(name)]
      end
      
      def subitem_extended_properties_sql(subitem)
        subitem.extended_properties.each_pair.map do |name, value|
          Mkxms::Mssql::Property.addition_sql(
            name, value,
            property_subject_identifiers + [subitem.class::SQL_OBJECT_TYPE.upcase, Utils.unquoted_name(subitem.name)]
          )
        end
      end
    end
  end

  class PropertyHandler
    module ElementHandler
      def handle_property_element(parse)
        parse.context = PropertyHandler.new(self, parse.node.attributes)
      end
    end
    
    def initialize(describable, attrs)
      @describable = describable
      @name = attrs['name']
      @value_type = attrs['type'].downcase
    end
    
    def handle_text(property_value, node)
      stored_value = property_value.dup
      
      stored_value = Base64.decode64(stored_value) if @value_type.include? 'binary'
      
      stored_value.define_singleton_method(
        :to_sql_literal,
        &(case @value_type
        when 'char', 'varchar', 'uniqueidentifier', 'smalldatetime', 'datetime'
          ->() {"'#{self}'"}
        when 'nchar', 'nvarchar'
          ->() {"N'#{self}'"}
        when 'binary', 'varbinary'
          ->() {"0x" + self.bytes.map {|b| "%02x" % b}.join}
        else
          ->() {self.to_s}
        end)
      )
      
      @describable.extended_properties[@name] = stored_value
    end
  end
end
