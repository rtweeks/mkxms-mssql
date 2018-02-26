require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Synonym
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'SYNONYM'
    
    def initialize(schema, name, referent)
      @schema = schema
      @name = name
      @referent = referent
    end
    
    attr_accessor :schema, :name, :referent
    
    def to_sql
      [].tap do |lines|
        lines << "CREATE SYNONYM #{qualified_name} FOR #{referent};"
        lines.concat extended_properties_sql
      end.join("\n")
    end
  end
  
  class SynonymHandler
    include PropertyHandler::ElementHandler
    
    def initialize(synonyms, node)
      a = node.attributes
      Synonym.new(a['schema'], a['name'], a['for']).tap do |syn|
        store_properties_on syn
        synonyms << (@synonym = syn)
      end
    end
  end
end
