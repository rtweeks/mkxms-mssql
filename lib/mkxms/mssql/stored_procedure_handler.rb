require 'mkxms/mssql/access_object_definition'
require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms::Mssql
  class StoredProcedure
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'PROCEDURE'
    
    def initialize(attrs)
      @schema = attrs['schema']
      @name = attrs['name']
      @definition = ''
      @param_properties = Hash.new {|h, k| h[k] = ''}
    end
    
    attr_accessor :schema, :name
    attr_reader :definition, :param_properties
    
    def to_sql
      mvdef = AccessObjectDefinition.replace_object_name(definition, "[{filename}]")
      ([mvdef] + extended_properties_sql + param_properties_sql).join("\n")
    end
    
    def param_properties_sql
      @param_properties.each_pair.map do |k, v|
        Property.addition_sql(k[1], v, property_subject_identifiers + ['PARAMETER', Utils.unquoted_name(k[0])])
      end
    end
  end

  class StoredProcedureHandler
    include PropertyHandler::ElementHandler
    
    def initialize(procedures, node)
      a = node.attributes
      
      @procedure = StoredProcedure.new(a).tap do |sp|
        procedures << sp
      end
    end
    
    def extended_properties
      @procedure.extended_properties
    end
    
    def handle_definition_element(parse); end
    
    def handle_references_element(parse); end
    
    def handle_param_property_element(parse); end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'definition']
        @procedure.definition << text
      when ['', 'param-property']
        a = parent_element.attributes
        @procedure.param_properties[[a['param'], a['property']]] << text
      end
    end
  end
end
