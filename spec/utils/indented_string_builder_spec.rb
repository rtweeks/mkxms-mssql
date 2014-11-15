require 'mkxms/mssql/indented_string_builder'

class RSpec::Expectations::ExpectationTarget
  def to_have(length_expectation)
    (match = Object.new).extend RSpec::Matchers
    to match.have length_expectation
  end
  
  def to_not_have(length_expectation)
    (match = Object.new).extend RSpec::Matchers
    to_not match.have length_expectation
  end
end

RSpec::Matchers.define :have do |expected|
  match do |actual|
    actual.length == expected.count
  end
  failure_message do |actual|
    "expected #{actual} to have #{expected.count} items"
  end
end

RSpec::Expectations::LengthExpectation = Struct.new(:count)
class Integer
  def items
    RSpec::Expectations::LengthExpectation.new(self)
  end
  
  def item
    raise "Numericity mismatch" unless self == 1
    RSpec::Expectations::LengthExpectation.new(1)
  end
end

describe Mkxms::Mssql::IndentedStringBuilder do
  def check_contractor_example(s)
    lines = s.to_s.lines
    expect(lines).to_have 4.items
    ["SELECT", "  name", "  AND trade", ";"].each.with_index do |line_start, i|
      expect(lines[i]).to start_with(line_start)
    end
  end
  
  it "can print a single line like a regular StringIO" do
    subject.puts("Hello world!")
    expect(subject.to_s.lines).to_have 1.item
  end
  
  it "can substitute into a line" do
    sub = "happy"
    subject.puts("I'm feeling %s.") {sub}
    expect(subject.to_s).to include(sub)
    expect(subject.to_s.lines).to_have 1.item
  end
  
  it "can substitue a value that is not a string into a template" do
    sub = 17
    subject.puts("I would like %s candy bars.") {sub}
    expect(subject.to_s).to include(sub.to_s)
    expect(subject.to_s.lines).to_have 1.item
  end
  
  it "can substitute an indented section" do
    conditions = "name = 'Joe'\nAND trade='plumber'"
    subject.puts("SELECT * FROM contractors WHERE %s;", :each_indent=>'  ') {conditions}
    expect(subject.to_s.lines).to_have 4.items
    check_contractor_example(subject)
  end
  
  it "can #puts without substitution" do
    subject.puts("weird %stuff", :sub => nil)
    expect(subject.to_s).to eql("weird %stuff\n")
  end
  
  context "begin/end template" do
    let(:template) {"BEGIN".."END"}
    
    it "substitutes a single line correctly" do
      subject.puts(template) {"body"}
      expect(subject.to_s).to eql("BEGIN\n  body\nEND\n")
    end
    
    it "substitutes multiple lines correctly" do
      subject.puts(template) {"body1\nbody2"}
      expect(subject.to_s).to eql("BEGIN\n  body1\n  body2\nEND\n")
    end
  end
  
  it "yields the matched name if the :sub option is :named" do
    sub_indicators = []
    subject.puts("Eeny {meeny} {miney} mo", :sub => :named) {|insert|
      sub_indicators << insert
      "woot!"
    }
    expect(sub_indicators).to include('meeny')
    expect(sub_indicators).to include('miney')
  end
  
  context "DSL" do
    it "reroutes #puts to the builder (not $stdout)" do
      expect do
        subject.dsl {
          puts "Hello, world!"
        }
      end.not_to output.to_stdout
      expect(subject.to_s).to eql("Hello, world!\n")
    end
    
    it "provides for indented blocks" do
      subject.dsl {
        puts "SELECT * FROM contractors WHERE"
        indented {
          puts "name = 'Joe'"
          puts "AND trade = 'plumber'"
        }
        puts ";"
      }
      check_contractor_example(subject)
    end
    
    it "allows indented injection with #puts" do
      subject.dsl {
        puts "SELECT * FROM contractors WHERE %s;" do
          puts "name = 'Joe'"
          puts "AND trade = 'plumber'"
        end
      }
      check_contractor_example(subject)
    end
    
    it "captures variables from surrounding binding" do
      the_answer = 42
      subject.dsl {
        puts "The answer to the ultimate question of life, the universe, and everything is #{the_answer}."
      }
      expect(subject.to_s).to include(the_answer.to_s)
    end
    
    it "allows calls to methods defined in the surrounding binding" do
      def blargize(s)
        s + "blarg"
      end
      
      subject.dsl {
        puts "All answers exist on the #{blargize 'web'}."
      }
    end
    
    it "does not capture output to $stdout within methods of the surrounding binding" do
      def real_puts(s)
        puts s
      end
      
      expect {
        subject.dsl {
          real_puts "This goes out!"
        }
      }.to output.to_stdout
    end
    
    it "is available from the class and returns a string" do
      class_dsl_result = subject.class.dsl {
        puts "Hello, world!"
      }
      
      expect(class_dsl_result).to eql("Hello, world!\n")
    end
    
    context "in subclass" do
      it "properly handles calls to methods nested within DSL" do
        test_class = Class.new(subject.class) do
          def initialize
            super
            
            dsl {
              puts "BEGIN"
              indented {
                add_command
              }
              puts "END"
            }
          end
          
          def add_command
            dsl {
              puts "command"
            }
          end
        end
        
        test_instance = test_class.new
        expect(test_instance.to_s).to eql("BEGIN\n  command\nEND\n")
      end
      
      it "property handles plain #puts calls within a called method nested within DSL" do
        test_class = Class.new(subject.class) do
          def initialize
            super
            
            dsl {
              puts "BEGIN"
              indented {add_command}
              puts "END"
            }
          end
          
          def add_command
            puts "command"
          end
        end
        
        test_instance = test_class.new
        expect(test_instance.to_s).to eql("BEGIN\n  command\nEND\n")
      end
    end
  end
end
