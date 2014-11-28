require "rexml/document"
require "rexml/element"
require "mkxms/mssql/utils"

module Mkxms; end

module Mkxms::Mssql
  class Mkxms::Mssql::Engine
    ParseItem = Struct.new(:context, :node) do
      def delegate_to(klass, *constructor_args)
        args = constructor_args + [node]
        self.context = klass.new(*args)
      end
    end
    
    MissingHandler = Struct.new(:context_class, :handler_name) do
      def to_s
        "#{self.context_class.name} does not define #{self.handler_name}"
      end
    end
    
    class ParseErrors < Exception
      def initialize(errors)
        @errors = errors
        super(@errors.map(&:to_s).join("\n"))
      end
      
      attr_reader :errors
    end
    
    def initialize(document, initial_context)
      @initial_context = initial_context
      @parse_items = [ParseItem.new(initial_context, document.root)]
      @missing_handlers = []
    end
    
    attr_reader :missing_handlers
    
    def run
      until @parse_items.empty?
        parse_item
      end
      
      errors = @missing_handlers
      raise ParseErrors.new(errors) unless errors.empty?
    end
    
    private
    def parse_item
      item = @parse_items.shift
      case item.node
      when REXML::Element
        begin
          handler = item.context.method(handler_name = element_handler_method_name(item.node))
        rescue NameError
          record_missing_handler(item.context.class, handler_name)
          return
        end
        result = ParseItem.new(item.context, item.node)
        handler[result]
        @parse_items = item.node.children.select do |node|
          [REXML::Element, REXML::Text].any? {|c| node.kind_of? c}
        end.map do |node|
          ParseItem.new(result.context, node)
        end + @parse_items
      when REXML::Text
        begin
          handler = item.context.method(:handle_text)
        rescue
          record_missing_handler(item.context.class, :handle_text) unless item.node.value =~ /^\s*$/
          return
        end
        handler[item.node.value, item.node.parent]
      end
    end
    
    def element_handler_method_name(node)
      case node
      when REXML::Element
        "handle_#{Utils.code_sym_for node.name}_element".to_sym
      end
    end
    
    def record_missing_handler(context_class, method_name)
      @missing_handlers << MissingHandler.new(context_class, method_name)
    end
  end
end
