require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Role
    include ExtendedProperties, Property::Hosting
    
    def initialize(name, owner: nil)
      @name = name
      @owner = owner
      @encompassing_roles = []
    end
    
    attr_accessor :name, :owner
    attr_reader :encompassing_roles
    
    def definition_sql
      "CREATE ROLE #{name};" + extended_properties_sql.joined_on_new_lines
    end
    
    def authorization_sql
      "ALTER AUTHORIZATION ON ROLE:: #{name} TO #{owner};" if owner
    end
    
    def membership_sql
      encompassing_roles.map do |encompassing_role|
        "EXEC sp_addrolemember '#{Utils.unquoted_name encompassing_role}', '#{Utils.unquoted_name name}';\n"
      end.join('')
    end
    
    def property_subject_identifiers
      ['USER', Utils.unquoted_name(name)]
    end
  end
  
  class RoleHandler
    include PropertyHandler::ElementHandler
    
    def initialize(roles, node)
      @role = Role.new(node.attributes['name'], owner: node.attributes['owner']).tap do |r|
        roles << r
      end
    end
    
    def extended_properties
      @role.extended_properties
    end
    
    def handle_member_of_element(parse)
      @role.encompassing_roles << parse.node.attributes['name']
    end
  end
end
