require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  Reference = Struct.new(:schema, :name) do
    include Utils::SchemaQualifiedName
  end
  
  module Dependencies
    def dependencies
      @dependencies ||= []
    end
  end
  
  class ReferencesHandler
    module ElementHandler
      def handle_references_element(parse)
        a = parse.node.attributes
        referrer.dependencies << Reference.new(a['schema'], a['name'])
      end
    end
  end
end
