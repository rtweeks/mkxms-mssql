require 'mkxms/mssql/sql_string_manipulators'

describe Mkxms::Mssql::SqlStringManipulators do
  module T
    extend Mkxms::Mssql::SqlStringManipulators
  end
  
  context "dedenting" do
    it "handles a string with no newlines" do
      expect(T.dedent("foo")).to eql("foo")
    end
  end
end
