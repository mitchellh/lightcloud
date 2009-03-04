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
  #
  # Sets a value to a key in the LightCloud system.
  #
  # Set first checks to see if the key is already stored. If it is
  # it uses that same node to store the new value. Otherwise, it
  # determines where to store the value based on the hash_ring
  def self.set(key, value, system=DEFAULT_SYSTEM)
    storage_node = self.locate_node_or_init(key, system)
    return storage_node.set(key, value)
  end

  #--
  # Lookup Cloud
  #++
  def self.locate_node_or_init(key, system)
    storage_node = self.locate_node(key, system)

    if storage_node.nil?
      storage_node = self.get_storage_ring(system).get_node(key)

      lookup_node = self.get_lookup_ring(system).get_node(key)
      lookup_node.set(key, storage_node.to_s)
    end

    storage_node
  end

  #
  # Locates a node in the lookup ring, returning the node if it is found, or
  # nil otherwise.
  def self.locate_node(key, system=DEFAULT_SYSTEM)
    nodes = self.get_lookup_ring(system).iterate_nodes(key)
    
    lookups = 0
    value = nil
    nodes.each_index do |i|
      lookups = i
      return nil if lookups > 2
      
      node = nodes[lookups]
      value = node.get(key)

      break unless value.nil?
    end

    return nil if value.nil?
    
    if lookups == 0
      return self.get_storage_node(value, system)
    else
      return self._clean_up_ring(key, value, system)
    end
  end

  def self._clean_up_ring(key, value, system)
    nodes = self.get_lookup_ring(system).iterate_nodes(key)

    nodes.each_index do |i|
      break if i > 1

      node = nodes[i]
      if i == 0
        node.set(key, value)
      else
        node.delete(key)
      end
    end

    return self.get_storage_node(value, system)
  end

  #--
  # Accessors for rings
  #++
  def self.get_lookup_ring(system=DEFAULT_SYSTEM)
    @@systems[system][0]
  end

  def self.get_storage_ring(system=DEFAULT_SYSTEM)
    @@systems[system][1]
  end

  #--
  # Accessors for nodes
  #++
  def self.get_storage_node(name, system=DEFAULT_SYSTEM)
    @@systems[system][3].get(name)
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
