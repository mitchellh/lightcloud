######################################
# LightCloud
# Code ported from Python version written by Amir Salihefendic
######################################
# Copyright (c) 2009, Mitchell Hashimoto, mitchell.hashimoto@gmail.com
#

require 'rubygems'
require 'hash_ring'
require 'rufus/tokyo'

require File.join(File.dirname(__FILE__), 'tyrant_client')

# = LightCloud Library
#
# == Background
#
# == Usage
#
#  require 'rubygems'
#  require 'lightcloud'
#
class LightCloud
  VERSION = '0.1'
  DEFAULT_SYSTEM = 'default'

  @@systems = {}

  def self.init(lookup_nodes, storage_nodes, system=DEFAULT_SYSTEM)
    lookup_ring, name_to_l_nodes = self.generate_ring(lookup_nodes)
    storage_ring, name_to_s_nodes = self.generate_ring(storage_nodes)

    @@systems[system] = [lookup_ring, storage_ring, name_to_l_nodes, name_to_s_nodes]
  end

  #--
  # Get/Set/Delete
  #++
  def self.set(key, value, system=DEFAULT_SYSTEM)
    
  end

  #--
  # Helpers
  #++
  def self.generate_nodes(config)
    lookup_nodes = {}
    storage_nodes = {}

    config.each do |k,v|
      if k.include?("lookup")
        lookup_nodes[k] = v
      elsif k.include?("storage")
        storage_nodes[k] = v
      end
    end

    return lookup_nodes, storage_nodes
  end
  
  #
  # Given a set of nodes it creates the the nodes as Tokyo
  # Tyrant objects and returns a hash ring with them
  def self.generate_ring(nodes)
    objects = []
    name_to_obj = {}
    
    nodes.each do |name, nodelist|
      obj = TyrantNode.new(name, nodelist)
      name_to_obj[name] = obj

      objects.push(obj)
    end

    return HashRing.new(objects), name_to_obj
  end
end
