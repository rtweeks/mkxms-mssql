require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/clr_impl'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class ClrFunction
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'FUNCTION'
    
    class ResultTable
      extend Utils::InitializedAttributes
      
      attr_init(:columns) {[]}
      
      class Column
        include ExtendedProperties
        
        SQL_OBJECT_TYPE = 'COLUMN'
        
        def initialize(name, result_type)
          @name = name
          @result_type = result_type
        end
        
        attr_accessor :name, :result_type
        
        def type_spec
          result_type.type_spec
        end
      end
    end
    
    def initialize(attrs)
      @schema = attrs['schema']
      @name = attrs['name']
      @execute_as = attrs['execute-as']
    end
    
    attr_accessor :schema, :name, :execute_as, :clr_impl, :returns, :result_table
    attr_init(:params) {[]}
    
    def to_sql
      (procedure_def_sql + extended_properties_sql + param_properties_sql + result_column_properties_sql).join("\n")
    end
    
    def procedure_def_sql
      [[].tap do |lines|
        lines << "CREATE FUNCTION [{filename}] ("
        lines << params.map do |param|
          "  #{param.name} #{param.type_spec}".tap do |param_spec|
            param_spec << " = #{param.default_value}" if param.default_value
          end
        end.join(",\n")
        lines << ")"
        case 
        when returns
          lines << "RETURNS #{returns.type_spec}"
        when result_table
          lines << "RETURNS TABLE ("
          lines << result_table.columns.map do |col|
            "  #{col.name} #{col.type_spec}"
          end.join(",\n")
          lines << ")"
        else
          raise RuntimeError.new("Function return not defined")
        end
        case execute_as
        when "OWNER"
          lines << "WITH EXECUTE AS OWNER"
        when String
          lines << "WITH EXECUTE AS '#{Utils.unquoted_name execute_as}'"
        end
        lines << "AS EXTERNAL NAME #{clr_impl.full_specifier};"
      end.join("\n")]
    end
    
    def param_properties_sql
      params.map do |param|
        subitem_extended_properties_sql(param)
      end
    end
    
    def result_column_properties_sql
      return [] unless result_table
      result_table.columns.map do |col|
        subitem_extended_properties_sql(col)
      end
    end
  end
  
  class ClrFunctionHandler
    include PropertyHandler::ElementHandler
    
    class ResultTableColumnHandler
      include PropertyHandler::ElementHandler
      
      def initialize(column)
        @column = column
      end
      
      def extended_properties
        @column.extended_properties
      end
    end
    
    def initialize(functions, node)
      a = node.attributes
      
      @function = ClrFunction.new(a).tap do |f|
        functions << f
      end
    end
    
    def extended_properties
      @function.extended_properties
    end
    
    def handle_implementation_element(parse)
      a = parse.node.attributes
      @function.clr_impl = ClrMethod.new(a['assembly'], a['class'], a['method'])
    end
    
    def handle_parameter_element(parse)
      a = parse.node.attributes
      Parameter.new(
        a['name'],
        a['type-schema'],
        a['type'],
        a['capacity'],
        a['precision'],
        a['scale'],
        a['default'],
        a['output'],
      ).tap do |param|
        @function.params << param
        parse.context = ParameterHandler.new(param)
      end
    end
    
    def handle_returns_element(parse)
      a = parse.node.attributes
      @function.returns = ResultType.new(
        a['type-schema'],
        a['type'],
        a['capacity'],
        a['precision'],
        a['scale'],
      )
    end
    
    def handle_result_table_element(parse)
      @function.result_table = @result_table = ClrFunction::ResultTable.new
    end
    
    def handle_column_element(parse)
      a = parse.node.attributes
      ClrFunction::ResultTable::Column.new(
        a['name'],
        ResultType.new(
          a['type-schema'],
          a['type'],
          a['capacity'],
          a['precision'],
          a['scale'],
          a['collation'],
        )
      ).tap do |col|
        @result_table.columns << col
        # Dispatch parse for column properties
        parse.context = ResultTableColumnHandler.new(col)
      end
    end
  end
end
