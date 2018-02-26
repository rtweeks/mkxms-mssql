require 'mkxms/mssql/clr_impl'
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
    
    attr_accessor :schema, :name, :table, :timing, :execute_as, :disabled, :not_replicable, :clr_impl
    attr_init(:events) {[]}
    attr_init(:definition) {""}
    
    def to_sql
      def_sql = clr_impl ? clr_definition : definition
      [def_sql.expand_tabs.gsub(/ +\n/, "\n")].tap do |result|
        unless (ep_sql = extended_properties_sql).empty?
          result << ep_sql.joined_on_new_lines
        end
        if disabled
          result << "DISABLE TRIGGER #{qualified_name} ON #{table.qualified_name};"
        end
      end.join(ddl_block_separator)
    end
    
    def clr_definition
      [].tap do |lines|
        lines << "CREATE TRIGGER #{schema}.#{name}"
        lines << "ON #{table.qualified_name}"
        case execute_as
        when 'OWNER'
          lines << "WITH EXECUTE AS OWNER"
        when String
          lines << "WITH EXECUTE AS #{execute_as.sql_quoted}"
        end
        lines << "#{timing} #{events.join(', ')}"
        lines << "NOT FOR REPLICATION" if not_replicable
        lines << "AS EXTERNAL NAME #{clr_impl.full_specifier};"
      end.join("\n")
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
    
    # This function handles a CLR implementation
    def handle_implementation_element(parse)
      a = parse.node.attributes
      @trigger.clr_impl = ClrMethod.new(a['assembly'], a['class'], a['method'])
    end
  end
end
