require 'mkxms/mssql/property_handler'

module Mkxms; end

module Mkxms::Mssql
  class Schema
    include ExtendedProperties, Property::Hosting
    
    def initialize(name, owner: nil)
      @name = name
      @owner = owner
    end
    
    attr_accessor :name, :owner
    
    def to_sql
      if owner
        "CREATE SCHEMA #{name} AUTHORIZATION #{owner};"
      else
        "CREATE SCHEMA #{name};"
      end + extended_properties_sql.joined_on_new_lines
    end
    
    def property_subject_identifiers
      ['SCHEMA', Utils.unquoted_name(name)]
    end
  end

  class SchemaHandler
    include PropertyHandler::ElementHandler
    
    def initialize(schemas, node)
      @schema = Schema.new(node.attributes['name'], owner: node.attributes['owner']).tap do |s|
        schemas << s
      end
    end
    
    def extended_properties
      @schema.extended_properties
    end
  end
end
