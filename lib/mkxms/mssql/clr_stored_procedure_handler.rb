require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/clr_impl'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class ClrStoredProcedure
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    
    SQL_OBJECT_TYPE = 'PROCEDURE'
    
    def initialize(attrs)
      @schema = attrs['schema']
      @name = attrs['name']
      @execute_as = attrs['execute-as']
    end
    
    attr_accessor :schema, :name, :clr_impl, :execute_as
    attr_init(:params) {[]}
    
    def to_sql
      (procedure_def_sql + extended_properties_sql + param_properties_sql).join("\n")
    end
    
    def procedure_def_sql
      [[].tap do |lines|
        lines << "CREATE PROCEDURE [{filename}]"
        lines << params.map do |param|
          "  #{param.name} #{param.type_spec}".tap do |param_spec|
            param_spec << " = #{param.default_value}" if param.default_value
            param_spec << " OUT" if param.output
          end
        end.join(",\n")
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
      end.flatten
    end
  end
  
  class ClrStoredProcedureHandler
    include PropertyHandler::ElementHandler
    
    def initialize(procedures, node)
      a = node.attributes
      
      @procedure = ClrStoredProcedure.new(a).tap do |sp|
        procedures << sp
      end
    end
    
    def extended_properties
      @procedure.extended_properties
    end
    
    def handle_implementation_element(parse)
      a = parse.node.attributes
      @procedure.clr_impl = ClrMethod.new(a['assembly'], a['class'], a['method'])
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
        @procedure.params << param
        parse.context = ParameterHandler.new(param)
      end
    end
  end
end
