require 'mkxms/mssql/property_handler'

module Mkxms; end

module Mkxms::Mssql
  ClrMethod = Struct.new(:assembly, :asm_class, :method) do
    def full_specifier
      to_a.join('.')
    end
  end
  
  # The Parameter class(es) are defined here because they are only important
  # for CLR-linked objects
  Parameter = Struct.new(
    :name,
    :type_schema, :type, :capacity, :precision, :scale,
    :default_value,
    :output
  ) do
    include ExtendedProperties
    
    SQL_OBJECT_TYPE = 'PARAMETER'
    
    def type_spec
      [type_schema, type].compact.join(".").tap do |result|
        result << "(#{capacity})" if capacity
        result << "(#{[precision, scale].compact.join(', ')})" if precision
      end
    end
  end
  
  class ParameterHandler
    include PropertyHandler::ElementHandler
    
    def initialize(parameter)
      @parameter = parameter
    end
    
    attr_reader parameter
  end
end
