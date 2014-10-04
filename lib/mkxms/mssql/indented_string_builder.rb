require 'forwardable'
require 'stringio'

module Mkxms; end

module Mkxms::Mssql
  class IndentedStringBuilder
    NAMED_SUBSTITUTIONS = /\{(?<name>\S+)\}/
    
    class LineAccumulator
      def initialize(indent, &flush_to)
        @indent = indent
        @flush_to = flush_to
        @value = @indent.dup
      end
      
      def flush
        @flush_to[@value]
        @value = @indent.dup
      end
      
      def any_acculumation?
        @value != @indent
      end
      
      def <<(v)
        @value << v
        return self
      end
    end
    
    class DSL
      extend Forwardable
      
      def self.for(builder, block)
        new(builder, block.binding).instance_eval(&block)
      end
      
      def initialize(builder, outer_binding)
        @builder = builder
        @outer_binding = outer_binding
      end
      
      def_delegators :@builder, :indented
      
      def puts(*args, &blk)
        if blk
          @builder.puts(*args) {IndentedStringBuilder.new.tap {|i| i.dsl(&blk)}}
        else
          @builder.puts(*args)
        end
      end
      
      private
      def method_missing(sym, *args)
        if args.empty?
          @outer_binding.eval sym.to_s
        else
          @outer_binding.eval("method(:#{sym})").call(*args)
        end
      end
    end
    
    def initialize(options = {})
      @io = StringIO.new
      @indent = 0
      @each_indent = options.fetch(:each_indent, "  ")
    end
    
    def to_s
      @io.string
    end
    
    def indented(n = 1)
      prev_indent = @indent
      @indent += n
      begin
        yield
      ensure
        @indent = prev_indent
      end
    end
    
    def puts(s, options = {})
      sub_pattern = options.fetch(:sub, '%s')
      sub_pattern = NAMED_SUBSTITUTIONS if sub_pattern == :named
      if sub_pattern.nil?
        @io.puts(s)
        return s.each_line.lazy.map {|l| l.chomp}
      end
      
      sub_pattern = Regexp.compile(Regexp.escape(sub_pattern)) unless sub_pattern.is_a? Regexp
      sub_name_index = sub_pattern.named_captures.fetch('name', []).min
      scan_pattern = Regexp.union(sub_pattern, /\n|$/)
      current_indent = indent_string
      
      i = Enumerator.new {|y| v = 0; loop {y.yield(v); v += 1}}
      completed = 0

      lines = Enumerator.new do |e|
        if s.is_a? Range
          e.yield(current_indent + s.begin)
          body = yield
          hanging_indent = current_indent + @each_indent
          if body.is_a?(String) || !body.respond_to?(:each)
            body = body.to_s.each_line
          end
          body.each {|l| e.yield(hanging_indent + l.chomp)}
          e.yield(current_indent + s.end)
          next
        end
        
        line = LineAccumulator.new(current_indent) {|v| e.yield v}
        subbed_multiline = false
        s.scan(scan_pattern) do |m|
          chunk_range, completed = completed...$~.begin(0), $~.end(0)
          chunk_empty = !chunk_range.cover?(chunk_range.begin)
          
          case
          when m == "\n"
            (line << s[chunk_range]).flush if line.any_acculumation? || !chunk_empty
            next
          when m == "" && !chunk_empty
            (line << s[chunk_range]).flush
            next
          when m == ""
            line.flush if line.any_acculumation?
            next
          end
          
          # Expect repl is a string or an enumerator of lines
          prev_indent = @indent
          @indent += 1
          hanging_indent = indent_string
          begin
            yield_from_string = proc do |r|
              s_chunk = s[chunk_range]
              if r.include? "\n"
                line << s_chunk if line.any_acculumation? || !s_chunk.match(/^\s*$/)
                line.flush if line.any_acculumation?
                r.each_line {|l| e.yield(hanging_indent + l.chomp)}
              else
                line << s_chunk << r
              end
            end
            
            yield_args = []
            yield_args << m[sub_name_index - 1] if sub_name_index
            yield_args << i.next
            case repl = yield(*yield_args)
            when String
              yield_from_string[repl]
            when ->(v){v.respond_to? :each}
              (line << s[chunk_range]).flush
              repl.each {|l| e.yield(
                hanging_indent + 
                l.chomp.gsub("\n", hanging_indent + "\n")
              )}
            else
              yield_from_string[repl.to_s]
            end
          ensure
            @indent = prev_indent
          end
        end
      end
      
      lines.tap {|e| e.dup.each {|l| @io.puts l}}
    end
    
    def dsl(&block)
      DSL.for(self, block)
      return self
    end
    
    def self.dsl(&block)
      new.dsl(&block).to_s
    end
    
    def each
      @io.string.each_line {|l| yield l.chomp}
    end
    include Enumerable
    
    protected
    def indent_string
      @each_indent * @indent
    end
  end
end
