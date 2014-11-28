require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Statistics
    extend Utils::FlagsQueries
    include ExtendedProperties
    
    def initialize(attrs)
      @schema = attrs['in-schema']
      @relation = attrs['on']
      @name = attrs['name']
      @columns = []
      @flags = []
      
      @flags << :manual_recompute if attrs['no-recompute']
    end
    
    attr_accessor :schema, :relation, :name
    attr_reader :columns, :flags
    flags_query :manual_recompute
    
    def xmigra_params
      [qualified_relation, @columns.join(', ')].tap do |result|
        result << {'with' => 'NORECOMPUTE'} if manual_recompute?
      end
    end
    
    def name_params_pair
      [name, xmigra_params]
    end
    
    def qualified_relation
      "#@schema.#@relation"
    end
  end
  
  class StatisticsHandler
    include PropertyHandler::ElementHandler
    
    def initialize(statistics_objs, node)
      a = node.attributes
      
      @statistics = Statistics.new(a).tap do |s|
        statistics_objs << s
      end
    end
    
    def extended_properties
      @statistics.extended_properties
    end
    
    def handle_column_element(parse)
      @statistics.columns << parse.node.attributes['name']
    end
  end
end
