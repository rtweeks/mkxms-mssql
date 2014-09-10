require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  module AccessObjectDefinition
    class Scanner
      def initialize(dfn)
        @dfn = dfn
        @start = 0
      end
      
      attr_reader :last_match
      
      def next_is(re)
        if (m = re.match(@dfn, @start)) && (m.begin(0) <= @start)
          @start = m.end(0)
          return @last_match = m
        end
      end
      
      def remaining?
        @start < @dfn.length
      end
    end
    
    def self.replace_object_name(dfn, s)
      scan = Scanner.new(dfn)
      looking_for = :create
      name_start = name_end = nil
      while scan.remaining?
        case
        when scan.next_is(/\s+/) # whitespace
        when scan.next_is(/--.*?\n/) # one line comment
        when scan.next_is(%r{/\*.*?\*/}m) # delimited comment
          nil
        when looking_for.equal?(:create) && scan.next_is(/CREATE\s/i)
          looking_for = :object_type
        when looking_for.equal?(:object_type) && scan.next_is(/(VIEW|PROC(EDURE)?|FUNCTION)\s/i)
          looking_for = :object_name
        when looking_for.equal?(:object_name) && scan.next_is(/[a-z][a-z0-9_]*|\[([^\]]|\]\])+\]/i)
          name_start ||= scan.last_match.begin(0)
          name_end = scan.last_match.end(0)
          looking_for = :qualifier_mark
        when looking_for.equal?(:qualifier_mark) && scan.next_is(/\./)
          looking_for = :object_name
        when looking_for.equal?(:qualifier_mark) && scan.next_is(/[^.]/)
          break
        end
      end
      
      dfn.dup.tap do |result|
        result[name_start...name_end] = s
        
        # These two steps keep the SQL from being in double-quoted scalar format:
        result.gsub!(/\s+\n/, "\n")
        result.replace(Utils.expand_tabs(result, tab_width: 4))
      end
    end
  end
end
