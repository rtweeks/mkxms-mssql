require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class PermissionGroup
    ACTION_STATEMENT_PROLOG_TEMPLATES = {
      'granted' => 'GRANT %s ON %s TO %s',
      'denied' => 'DENY %s ON %s TO %s',
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
          sql << ACTION_STATEMENT_PROLOG_TEMPLATES[action] % [p.name, p.target, subject]
          sql << ' WITH GRANT OPTION' if p.grant_option?
          sql << ';'
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
      @object = attrs['on']
      @column = attrs['column']
      @target = if @object
        "".tap do |subject|
          if @schema
            subject << (@schema + '.')
          end
          subject << @object
          subject << " (#@column)" if @column
        end
      else
        'DATABASE'
      end
      @grant_option = attrs['with-grant-option']
      @authority = attrs['by']
    end
    
    attr_accessor :name, :target_type, :name_scope, :column, :authority
    
    def target(scoped: true)
      if scoped && @name_scope
        "#@name_scope :: #@target"
      else
        @target
      end
    end
    
    def unscoped_target
      target(scoped: false)
    end
    
    def grant_option?
      @grant_option
    end
    def grant_option=(value)
      @grant_option = value
    end
    
    def object_id_parts
      [@target_type, @schema, @object, @column]
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
