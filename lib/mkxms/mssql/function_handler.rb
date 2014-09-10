require 'mkxms/mssql/access_object_definition'
require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Function
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'FUNCTION'
    
    def initialize(attrs)
      @schema = attrs['schema']
      @name = attrs['name']
      @definition = ''
      @references = []
      @param_properties = Hash.new {|h, k| h[k] = ''}
    end
    
    attr_accessor :schema, :name
    attr_reader :definition, :references, :param_properties
    
    def to_sql
      mvdef = AccessObjectDefinition.replace_object_name(definition, "[{filename}]")
      ([mvdef] + extended_properties_sql + param_properties_sql).join("\n")
    end
    
    def param_properties_sql
      @param_properties.each_pair.map do |k, v|
        Property.addition_sql(k[1], v, property_subject_identifiers + ['PARAMETER', Utils.unquoted_name(k[0])])
      end
    end
    
    def qualified_name
      "#@schema.#@name"
    end
  end

  class FunctionHandler
    include PropertyHandler::ElementHandler
    
    def initialize(functions, node)
      a = node.attributes
      
      @function = Function.new(a).tap do |f|
        functions << f
      end
    end
    
    def extended_properties
      @function.extended_properties
    end
    
    def handle_definition_element(parse); end
    
    def handle_references_element(parse)
      @function.references << %w[schema name].map {|k| parse.node.attributes[k]}.join('.')
    end
    
    def handle_param_property(parse); end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'definition']
        @function.definition << text
      when ['', 'param-property']
        a = parent_element.attributes
        @function.param_properties[[a['param'], a['property']]] << text
      end
    end
  end
end