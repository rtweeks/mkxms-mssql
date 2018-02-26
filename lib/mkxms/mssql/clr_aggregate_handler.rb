require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/clr_impl'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class ClrAggregate
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    extend Utils::InitializedAttributes
    
    SQL_OBJECT_TYPE = 'AGGREGATE'
    
    def initialize(attrs)
      @schema = attrs['schema']
      @name = attrs['name']
      @execute_as = attrs['execute-as']
    end
    
    attr_accessor :schema, :name, :execute_as, :clr_impl, :returns
    attr_init(:params) {[]}
    
    def to_sql
      (procedure_def_sql + extended_properties_sql + param_properties_sql)
    end
    
    def procedure_def_sql
      [[].tap do |lines|
        lines << "IF NOT EXISTS ("
        lines << "  SELECT * FROM xmigra.ignored_clr_assemblies asm"
        lines << "  WHERE asm.name = #{clr_impl.assembly.sql_quoted}"
        lines << ")"
        lines << "CREATE AGGREGATE [{filename}] ("
        lines << params.map do |param|
          "  #{param.name} #{param.type_spec}".tap do |param_spec|
            param_spec << " = #{param.default_value}" if param.default_value
          end
        end.join(",\n")
        lines << ")"
        lines << "RETURNS #{returns.type_spec}" if returns
        lines << "EXTERNAL NAME #{clr_impl.full_specifier};"
      end.join("\n")]
    end
    
    def param_properties_sql
      params.map do |param|
        subitem_extended_properties_sql(param)
      end
    end
  end
  
  class ClrArggregateHandler
    include PropertyHandler::ElementHandler
    
    def initialize(aggregates, node)
      a = node.attributes
      
      @aggregate = ClrAggregate.new(a).tap do |agg|
        store_properties_on agg
        aggregates << agg
      end
    end
    
    def handle_implementation_element(parse)
      a = parse.node.attributes
      @aggregate.clr_impl = ClrClass.new(a['assembly'], a['class'])
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
        @aggregate.params << param
        parse.context = ParameterHandler.new(param)
      end
    end
    
    def handle_returns_element(parse)
      a = parse.node.attributes
      @aggregate.returns = ResultType.new(
        a['type-schema'],
        a['type'],
        a['capacity'],
        a['precision'],
        a['scale'],
      )
    end
  end
end
