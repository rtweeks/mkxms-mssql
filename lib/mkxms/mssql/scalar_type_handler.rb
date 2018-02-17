require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class ScalarType
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'TYPE'
    
    def initialize(attrs)
      a = attrs
      @schema = a['schema']
      @name = a['name']
      @base_type = a['base-type']
      @capacity = a['capacity']
      @capacity = @capacity.to_i unless @capacity.nil? || @capacity == 'max'
      @precision = a['precision']
      @scale = a['scale']
      @nullable = !!a['nullable']
    end
    
    attr_accessor :schema, :name, :base_type, :capacity, :precision, :scale, :default
    
    def nullable?
      @nullable
    end
    
    def nullable=(val)
      @nullable = !!val
    end
    
    def type_spec
      base_type.dup.tap do |ts|
        case 
        when capacity
          ts << "(#{capacity})"
        when precision
          ts << "(#{[precision, scale].compact.join(", ")})"
        end
        ts << " NOT NULL" unless nullable
      end
    end
    
    def to_sql
      [].tap do |lines|
        lines << "CREATE TYPE #{qualified_name}"
        lines << "FROM #{type_spec};"
        
        if default
          lines << default.to_sql
          lines << "EXEC sp_bindefault #{default.qualified_name.sql_quoted}, #{qualified_name.sql_quoted};"
        end
      end.join("\n")
    end
    
    def element_size
      if %w[nchar nvarchar]
        2
      else
        1
      end
    end
  end
  
  class Default
    include Utils::SchemaQualifiedName
    
    def initialize(attrs)
      a = attrs
      @schema = a['schema']
      @name = a['name']
      @definition = ""
    end
    
    attr_accessor :schema, :name
    attr_reader :definition
    
    def to_sql
      "CREATE DEFAULT #{qualified_name} AS #{definition};"
    end
  end
  
  class ScalarTypeHandler
    include PropertyHandler::ElementHandler
    
    def initialize(user_types, node)
      a = node.attributes
      ScalarType.new(a).tap do |t|
        user_types << (@type = t)
      end
    end
    
    def extended_properties
      @type.extended_properties
    end
    
    def handle_default_element(parse)
      @type.default = Default.new(parse.node.attributes)
    end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'default']
        @type.default.definition << text
      end
    end
  end
end
