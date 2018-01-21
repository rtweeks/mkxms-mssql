require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Trigger
    extend Utils::InitializedAttributes
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    include XMigra::MSSQLSpecifics
    
    def initialize(schema, name, timing)
      @schema = schema
      @name = name
      @timing = timing
    end
    
    attr_accessor :schema, :name, :table, :timing
    attr_init(:events) {[]}
    attr_init(:definition) {""}
    
    def to_sql
      if (ep_sql = extended_properties_sql).empty?
        definition
      else
        definition + ddl_block_separator + ep_sql.joined_on_new_lines
      end
    end
  end
  
  TableRef = Struct.new(:schema, :name) do
    include Utils::SchemaQualifiedName
  end
  
  class DmlTriggerHandler
    extend XMigra::MSSQLSpecifics
    include PropertyHandler::ElementHandler
    
    def initialize(triggers, node)
      a = node.attributes
      
      @trigger = Trigger.new(a['schema'], a['name'], a['timing']).tap do |t|
        triggers << t
      end
    end
    
    def extended_properties
      @trigger.extended_properties
    end
    
    def handle_table_element(parse)
      a = parse.node.attributes
      @trigger.table = TableRef.new(a['schema'], a['name'])
    end
    
    def handle_event_element(parse)
      a = parse.node.attributes
      @trigger.events << a['type']
    end
    
    def handle_do_element(parse); end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'do']
        @trigger.definition << text.expand_tabs.gsub(/ +\n/, "\n")
      when ['', 'param-property']
        a = parent_element.attributes
        @trigger.param_properties[[a['param'], a['property']]] << text
      end
    end
  end
end
