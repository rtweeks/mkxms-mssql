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
  
  module StringHelpers
    def expand_tabs(tabstops_every = 8)
      self.lines.map do |l|
        if l.include?("\t")
          segs = l.split("\t")
          segs[0...-1].map do |seg|
            # seg length must _increase_ to a multiple of 8
            spaces_needed = tabstops_every - (seg.length + 1) % tabstops_every + 1
            seg + ' ' * spaces_needed
          end.join('') + segs[-1]
        else
          l
        end
      end.join('')
    end
    
    def sql_quoted
      %Q{N'#{gsub("'", "''")}'}
    end
  end
  
  # Primes in the interval [100, 255].  This enumerator can be queried by
  # classes that generate RAISERROR statements to provide unique-ish context
  # by passing the next multiple of one of the values taken from this
  # enumerator for each RAISERROR statement output (as a literal number in
  # the generated SQL).  This will assist
  RAISERROR_STATE_BASE = [
    101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163 ,167, 173,
    179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251
  ].each unless defined? RAISERROR_STATE_BASE
  
  # Create one instance of this class to write a sequence of similar
  # RAISERROR messages.  The state of each message will be unique within the
  # sequence until the 256th message.  The particular order is unique to
  # all other instances of this class.
  class RaiserrorWriter
    # Severity:
    #   11 is the minimum to transfer into a CATCH
    #   19 or higher can only be raised by sysadmin
    #   20 or higher is fatal to the connection
    
    SYMBOLIC_SEVERITIES = {
      :warning => 1,
      :error => 11,
    }
    
    def initialize(message, severity: 11)
      # Get a unique prime to use as an ideal to generate the 0-255 state-value
      # space.  With luck, the number is fairly unique to the message.
      severity = map_severity(severity)
      @state_base = RAISERROR_STATE_BASE.next
      @index = 1 # Start at 1 because 0 is the kernel -- every @state_base * 0 == 0
      @message = message
      @severity = severity
    end
    
    attr_reader :state_base
    attr_accessor :message, :severity
    
    def map_severity(value)
      SYMBOLIC_SEVERITIES.fetch(value, value)
    end
    
    def current_statement(*args, severity: nil)
      severity = map_severity(severity || self.severity)
      state_str = current_error_marker
      full_message = %Q{N'#{message.gsub("'", "''")} (search for "#{state_str}")'}
      trailing_args = ([state_str] + args.map(&:to_s)).join(', ')
      %Q{RAISERROR (#{full_message}, #{severity}, #{trailing_args})}.tap do |stmt|
        stmt.define_singleton_method(:error_marker) {state_str}
      end
    end
    
    def current_error_marker
      "/*ERR*/ #{current_state} /*ERR*/"
    end
    
    def current_state
      (state_base * @index) % 256
    end
    
    def next_statement(*args, **kwargs)
      current_statement(*args, **kwargs).tap {@index += 1}
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

class String
  include Mkxms::Mssql::Utils::StringHelpers
end
