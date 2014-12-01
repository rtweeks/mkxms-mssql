module Mkxms; end

module Mkxms::Mssql
  class QueryCursor
    def initialize(select_statement, variables, options = {})
      @select_statement = select_statement
      @select_statement += ';' unless @select_statement =~ /;\s*\Z/
      @cursor = options[:cursor_name] || self.class.generated_cursor_name
      @out = options[:output_to] || $stdout
      @indented = @out.respond_to?(:indented) ? @out.method(:indented) : ->(&blk) {blk.call}
      @global = options[:global]
      @indent = options[:indent] || '  '
      
      @variable_decl = variables.gsub(/\s+/, ' ')
      @variable_names = variables.split(',').map do |vardecl|
        vardecl.chomp.split(nil, 2)[0]
      end
    end
    
    attr_reader :cursor_name
    
    def each_row
      set_up_loop
      fetch_next
      @out.puts "WHILE @@FETCH_STATUS = 0"
      @out.puts "BEGIN"
      yield
      fetch_next(@indent)
      @out.puts "END;"
    end
    
    class ExpectedRowTest
      def initialize(test_proc)
        @test_proc = test_proc
      end
      
      def row(*args, &blk)
        @test_proc.call(*args, &blk)
      end
    end
    
    def expectations(opts = {})
      extra_action = expectation_failure_action(opts[:on_extra])
      test_entry_proc = if missing_action = expectation_failure_action(opts[:on_missing])
        proc {|&blk| test_entry(on_missing: missing_action, &blk)}
      else
        method(:test_entry)
      end
      
      set_up_loop
      
      yield ExpectedRowTest.new(test_entry_proc)
      
      if extra_action
        fetch_next
        @out.puts "IF @@FETCH_STATUS = 0"
        @out.puts "BEGIN"
        indented {extra_action.call}
        @out.puts "END;"
      end
      
      tear_down_loop
    end
    
    def test_entry(opts = {})
      opts = {} unless opts.kind_of? Hash
      missing_action = expectation_failure_action(opts[:on_missing]) || proc {}
      @out.puts
      fetch_next
      @out.puts "IF @@FETCH_STATUS <> 0"
      @out.puts "BEGIN"
      indented {missing_action.call}
      @out.puts "END ELSE BEGIN"
      indented {
        yield
      }
      @out.puts "END;"
    end
    
    def expectation_failure_action(value)
      case value
      when Proc then value
      when String then proc {@out.puts(@indent + value)}
      end
    end
    
    def cursor_scope(explicit_local = true)
      case
      when @global then 'GLOBAL'
      when explicit_local then 'LOCAL'
      else ''
      end
    end
    
    def set_up_loop
      @out.puts "DECLARE #@variable_decl;"
      @out.puts "DECLARE #@cursor CURSOR #{cursor_scope} FOR"
      @out.puts @select_statement
      @out.puts "OPEN #@cursor;"
    end
    
    def fetch_next(indent = '')
      @out.puts(indent + "FETCH NEXT FROM #@cursor INTO #{@variable_names.join(', ')};")
    end
    
    def tear_down_loop
      @out.puts "CLOSE #{cursor_scope(false)} #@cursor; DEALLOCATE #{cursor_scope(false)} #@cursor;"
    end
    
    def indented(&blk)
      @indented.call(&blk)
    end
    
    def self.generated_cursor_name
      @gensym_number ||= 0
      "gensym_cursor_#{@gensym_number += 1}"
    end
  end
end
