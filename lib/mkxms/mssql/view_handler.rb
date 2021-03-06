require 'mkxms/mssql/access_object_definition'
require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class View
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'VIEW'
    
    def initialize(attrs)
      @schema = attrs['schema']
      @name = attrs['name']
      @definition = ''
      @references = []
    end
    
    attr_accessor :schema, :name
    attr_reader :definition, :references
    
    def to_sql
      mvdef = AccessObjectDefinition.replace_object_name(definition, "[{filename}]")
      ([mvdef] + extended_properties_sql).join("\n")
    end
  end

  class ViewHandler
    include PropertyHandler::ElementHandler
    
    def initialize(views, node)
      a = node.attributes
      
      @view = View.new(a).tap do |v|
        store_properties_on v
        views << v
      end
    end
    
    def handle_definition_element(parse); end
    
    def handle_references_element(parse)
      @view.references << %w[schema name].map {|k| parse.node.attributes[k]}.join('.')
    end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'definition']
        @view.definition << text
      end
    end
  end
end
