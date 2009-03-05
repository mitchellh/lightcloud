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
#  LIGHT_CLOUD = {
#    'lookup1_A' => ['127.0.0.1:41401', '127.0.0.1:41402'],
#    'storage1_A' => ['192.168.0.2:51401', '192.168.0.2:51402']
#  }
#
#  lookup_nodes, storage_nodes = LightCloud.generate_nodes(LIGHT_CLOUD)
#  LightCloud.init(lookup_nodes, storage_nodes)
#
#  LightCloud.set("hello", "world")
#  print LightCloud.get("hello") # => world
#  LightCloud.delete("hello")
#
#  print LightCloud.get("hello") # => nil
#
class LightCloud
  VERSION = '0.7'
  DEFAULT_SYSTEM = 'default'
  @@instance = nil

  #--
  # INSTANCE METHODS
  #++
  # Initialize LightCloud as an instance instead of using the class
  # methods. Expects the same arguments as LightCloud.init, except
  # this will return a new instance of LightCloud.
  #
  # Any nodes initialized through here will not work one class methods.
  def initialize(lookup_nodes=nil, storage_nodes=nil, system=DEFAULT_SYSTEM)
    add_system(lookup_nodes, storage_nodes, system) if !lookup_nodes.nil? && !storage_nodes.nil?
  end

  #
  # Create a new LightCloud system within a pre-existing instance.
  def add_system(lookup_nodes, storage_nodes, system=DEFAULT_SYSTEM)
    lookup_ring, name_to_l_nodes = self.generate_ring(lookup_nodes)
    storage_ring, name_to_s_nodes = self.generate_ring(storage_nodes)

    @systems ||= {}
    @systems[system] = [lookup_ring, storage_ring, name_to_l_nodes, name_to_s_nodes]    
  end

  #--
  # Get/Set/Delete
  #++
  # Sets a value to a key on a LightCloud instance. See
  # LightCloud.set for more information
  def set(key, value, system=DEFAULT_SYSTEM)
    storage_node = locate_node_or_init(key, system)
    return storage_node.set(key, value)
  end

  #
  # Gets a value to a key on a LightCloud instance. See
  # LightCloud.get for more information.
  def get(key, system=DEFAULT_SYSTEM)
    result = nil

    # Try to lookup key directly
    storage_node = get_storage_ring(system).get_node(key)
    value = storage_node.get(key)

    result = value unless value.nil?
    
    # Else use the lookup ring
    if result.nil?
      storage_node = locate_node(key, system)
      
      result = storage_node.get(key) unless storage_node.nil?
    end

    result    
  end

  #
  # Delete a value from a LightCloud instance. See
  # LightCloud.delete for more information.
  def delete(key, system=DEFAULT_SYSTEM)
    storage_node = locate_node(key, system)
    
    storage_node = get_storage_ring(system).get_node(key) if storage_node.nil?
    lookup_nodes = get_lookup_ring(system).iterate_nodes(key)
    lookup_nodes.each_index do |i|
      break if i > 1
      
      lookup_nodes[i].delete(key)
    end
    
    storage_node.delete(key) unless storage_node.nil?
    true
  end

  #--
  # Lookup Cloud
  #++
  def locate_node_or_init(key, system)
    storage_node = locate_node(key, system)

    if storage_node.nil?
      storage_node = get_storage_ring(system).get_node(key)

      lookup_node = get_lookup_ring(system).get_node(key)
      lookup_node.set(key, storage_node.to_s)
    end

    storage_node
  end

  #
  # Locates a node in the lookup ring, returning the node if it is found, or
  # nil otherwise.
  def locate_node(key, system=DEFAULT_SYSTEM)
    nodes = get_lookup_ring(system).iterate_nodes(key)
    
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
      return get_storage_node(value, system)
    else
      return _clean_up_ring(key, value, system)
    end
  end

  def _clean_up_ring(key, value, system)
    nodes = get_lookup_ring(system).iterate_nodes(key)

    nodes.each_index do |i|
      break if i > 1

      node = nodes[i]
      if i == 0
        node.set(key, value)
      else
        node.delete(key)
      end
    end

    return get_storage_node(value, system)
  end

  #--
  # Accessors for rings
  #++
  def get_lookup_ring(system=DEFAULT_SYSTEM)
    @systems[system][0]
  end

  def get_storage_ring(system=DEFAULT_SYSTEM)
    @systems[system][1]
  end

  #--
  # Accessors for nodes
  #++
  def get_storage_node(name, system=DEFAULT_SYSTEM)
    @systems[system][3][name]
  end

  #
  # Given a set of nodes it creates the the nodes as Tokyo
  # Tyrant objects and returns a hash ring with them
  def generate_ring(nodes)
    objects = []
    name_to_obj = {}
    
    nodes.each do |name, nodelist|
      obj = TyrantNode.new(name, nodelist)
      name_to_obj[name] = obj

      objects.push(obj)
    end

    return HashRing.new(objects), name_to_obj
  end

  #--
  # CLASS METHODS
  #++
  #
  # Initializes LightCloud library with lookup and storage nodes.
  # This only needs to be called with servers with which you intend
  # to use the class methods (set/get/delete)
  def self.init(lookup_nodes, storage_nodes, system=DEFAULT_SYSTEM)
    instance.add_system(lookup_nodes, storage_nodes, system)
  end

  #
  # Sets a value to a key in the LightCloud system.
  #
  # Set first checks to see if the key is already stored. If it is
  # it uses that same node to store the new value. Otherwise, it
  # determines where to store the value based on the hash_ring
  def self.set(key, value, system=DEFAULT_SYSTEM)
    instance.set(key, value, system)
  end

  #
  # Gets a value based on a key. 
  def self.get(key, system=DEFAULT_SYSTEM)
    instance.get(key, system)
  end

  #
  # Lookup the key and delete it from both the storage ring
  # and lookup ring
  def self.delete(key, system=DEFAULT_SYSTEM)
    instance.delete(key, system)
  end

  #--
  # Instance accessor
  #++
  def self.instance
    @@instance ||= self.new
    @@instance
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
end
