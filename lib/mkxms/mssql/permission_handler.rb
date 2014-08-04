require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class PermissionGroup
    ACTION_STATEMENT_PROLOG_TEMPLATES = {
      'granted' => 'GRANT %s TO',
      'denied' => 'DENY %s TO',
    }
    
    def initialize(action, subject)
      @action = action
      @subject = subject
      @permissions = []
    end
    
    attr_accessor :action, :subject
    attr_reader :permissions
    
    def super_permissions_sql
      super_permissions.map do |p|
        ''.tap do |sql|
          sql << ACTION_STATEMENT_PROLOG_TEMPLATES[p.action] % [p.name]
          sql << ' WITH GRANT OPTION' if p.grant_option?
        end
      end
    end
    
    def regular_permissions_graph
      Hash.new.tap do |result|
        regular_permissions.sort {|a, b| a.target <=> b.target}.group_by {|p| p.target}.each_pair do |target, perms|
          result[target] = perms.map(&:name)
        end
      end
    end
    
    def is_super_permission?(p)
      action != 'granted' || p.grant_option?
    end
    
    def super_permissions
      permissions.select {|p| is_super_permission? p}
    end
    
    def regular_permissions
      permissions.select {|p| !is_super_permission? p}
    end
  end
  
  class Permission
    def initialize(attrs)
      @name = attrs['name']
      @target_type = attrs['target-type']
      @name_scope = attrs['name-scope']
      @schema = attrs['in-schema']
      @column = attrs['column']
      @target = if attrs.has_key?('on')
        [(@name_scope + ' :: ' if @name_scope), @schema, attrs['on']].compact.join('.').tap do |subject|
          subject << " (#@column)" if @column
        end
      else
        'DATABASE'
      end
      @grant_option = attrs['with-grant-option']
      @authority = attrs['by']
    end
    
    attr_accessor :name, :target_type, :name_scope, :target, :column, :authority
    
    def grant_option?
      @grant_option
    end
    def grant_option=(value)
      @grant_option = value
    end
  end
  
  class PermissionHandler
    def initialize(permissions, node)
      a = node.attributes
      
      @action = PermissionGroup.new(node.name, a['to'] || a['from']).tap do |pg|
        permissions << pg
      end
    end
    
    def handle_permission_element(parse)
      a = parse.node.attributes
      @action.permissions << Permission.new(a)
    end
  end
end
