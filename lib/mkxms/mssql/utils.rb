require 'tsort'

module Mkxms; end
module Mkxms::Mssql; end

module Mkxms::Mssql::Utils
  INVALID_NAME_CHAR = /[^A-Za-z0-9_]/
  
  module FlagsQueries
    def flags_query(*syms, flags: :flags)
      syms.each do |sym|
        define_method((sym.to_s + '?').to_sym) {send(flags).include? sym}
      end
    end
  end
  
  module InitializedAttributes
    def attr_init(*syms, &blk)
      raise "No block given for initialization of attr_init" unless blk
      
      syms.each do |sym|
        inst_var = "@#{sym}".to_sym
        define_method(sym) do
          instance_variable_get(inst_var) ||
          instance_variable_set(inst_var, blk[])
        end
      end
    end
  end
  
  module SchemaQualifiedName
    def qualified_name
      [schema, name].join('.')
    end
  end
  
  class NameRefGraph
    include TSort
    
    def initialize(items, children: :children)
      @items = items
      @children_message = children
    end
    
    def tsort_each_node(&blk)
      @items.each(&blk)
    end
    
    def tsort_each_child(item, &blk)
      item.send(@children_message).each(&blk)
    end
  end
end

class << Mkxms::Mssql::Utils
  def code_sym_for(s)
    s.gsub(self::INVALID_NAME_CHAR, '_').downcase.to_sym
  end
  
  def unquoted_name(s)
    return s unless s[0] == '[' && s[-1] == ']'
    return s[1...-1].gsub(']]', ']')
  end
  
  def newline_prefixed(s)
    "\n" + s
  end
  
  def chars_to_tab(prev, tab_width: 4)
    (prev.chars.length + 3) % 4 + 1
  end
  
  def expand_tabs(s, tab_width: 4)
    return s unless s.include? "\t"
    
    s.each_line.map do |l|
      while l.include? "\t"
        l.sub!("\t") {|m| ' ' * chars_to_tab($`, tab_width: tab_width)}
      end
      l
    end.join('')
  end
end
