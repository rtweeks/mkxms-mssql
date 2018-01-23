require 'mkxms/mssql/references_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Trigger
    extend Utils::InitializedAttributes
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Dependencies
    include Utils::SchemaQualifiedName
    include XMigra::MSSQLSpecifics
    
    def initialize(schema, name, timing, execute_as: nil, disabled: false, not_replicable: false)
      @schema = schema
      @name = name
      @timing = timing
      @execute_as = execute_as
      @disabled = disabled
      @not_replicable = not_replicable
    end
    
    attr_accessor :schema, :name, :table, :timing, :execute_as, :disabled, :not_replicable
    attr_init(:events) {[]}
    attr_init(:definition) {""}
    
    def to_sql
      [definition.expand_tabs.gsub(/ +\n/, "\n")].tap do |result|
        unless (ep_sql = extended_properties_sql).empty?
          result << ep_sql.joined_on_new_lines
        end
        if disabled
          result << "DISABLE TRIGGER #{qualified_name} ON #{table.qualified_name};"
        end
      end.join(ddl_block_separator)
    end
  end
  
  class DmlTriggerHandler
    extend XMigra::MSSQLSpecifics
    include PropertyHandler::ElementHandler
    include ReferencesHandler::ElementHandler
    
    def initialize(triggers, node)
      a = node.attributes
      
      @trigger = Trigger.new(
        a['schema'],
        a['name'],
        a['timing'],
        execute_as: a['execute_as'],
        disabled: a['disabled'],
        not_replicable: a['not-for-replication'],
      ).tap do |t|
        triggers << t
      end
    end
    
    def extended_properties
      @trigger.extended_properties
    end
    
    def dependencies
      @trigger.dependencies
    end
    
    def handle_table_element(parse)
      a = parse.node.attributes
      @trigger.table = Reference.new(a['schema'], a['name'])
    end
    
    def handle_event_element(parse)
      a = parse.node.attributes
      @trigger.events << a['type']
    end
    
    def handle_do_element(parse); end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'do']
        @trigger.definition << text
      end
    end
  end
end
