######################################
# hash_ring
# Code ported from Python version written by Amir Salihefendic
######################################
# Copyright (c) 2009, Mitchell Hashimoto, mitchell.hashimoto@gmail.com
#

require 'digest/md5'

# = HashRing Class
#
# == Background
#
# Implements consistent hashing that can be used when
# the number of server nodes can increase or decrease (like in memcached).
#
# Consistent hashing is a scheme that provides a hash table functionality
# in a way that the adding or removing of one slot
# does not significantly change the mapping of keys to slots.
#
# More information about consistent hashing can be read in these articles:
#
# "Web Caching with Consistent Hashing":
#    http://www8.org/w8-papers/2a-webserver/caching/paper2.html
#
# "Consistent hashing and random trees:
#    Distributed caching protocols for relieving hot spots on the World Wide Web (1997)":
#    http://citeseerx.ist.psu.edu/legacymapper?did=38148
#
# == Usage
#
#  memcache_servers = ['192.168.0.111:14107',
#                      '192.168.0.112:14107',
#                      '192.168.0.113:14108']
#
#  # Since server 1 has double the RAM, lets weight it
#  # twice as much to get twice the keys. This is optional
#  weights = { '192.168.0.111' => 2 }
#
#  ring = HashRing.new(memcache_servers, weights)
#  server = ring.get_node('my_key')
#
class HashRing
  VERSION = '0.1'

  #
  # Creates a HashRing instance
  #
  # == parameters
  #
  #   * nodes - A list of objects which have a proper to_s representation.
  #   * weights - A hash (dictionary, not to be mixed up with HashRing)
  #       which sets weights to the nodes. The default weight is that all
  #       nodes have equal weight.
  def initialize(nodes=nil, weights=nil)
    @ring = {}
    @_sorted_keys = []

    @nodes = nodes

    weights = {} if weights.nil?
    
    @weights = weights

    self._generate_circle()
    self
  end

  #
  # Generates the ring.
  #
  # This is for internal use only.
  def _generate_circle
    total_weight = 0
    
    @nodes.each do |node|
      total_weight += @weights[node] || 1
    end

    @nodes.each do |node|
      weight = @weights[node] || 1
      factor = ((40 * @nodes.length * weight) / total_weight.to_f).floor.to_i

      factor.times do |j|
        b_key = self._hash_digest("#{node}-#{j}")

        3.times do |i|
          key = self._hash_val(b_key) { |x| x+(i*4) }
          @ring[key] = node
          @_sorted_keys.push(key)
        end
      end
    end

    @_sorted_keys.sort!
  end

  #
  # Given a string key a corresponding node is returned. If the
  # ring is empty, nil is returned.
  def get_node(string_key)
    pos = self.get_node_pos(string_key)
    return nil if pos.nil?

    return @ring[@_sorted_keys[pos]]
  end

  #
  # Given a string key a corresponding node's position in the ring
  # is returned. Nil is returned if the ring is empty.
  def get_node_pos(string_key)
    return nil if @ring.empty?

    key = self.gen_key(string_key)
    nodes = @_sorted_keys
    pos = bisect(nodes, key)

    if pos == nodes.length
      return 0
    else
      return pos
    end
  end

  #
  # Returns an array of nodes where the key could be stored, starting
  # at the correct position.
  def iterate_nodes(string_key)
    returned_values = []
    pos = self.get_node_pos(string_key)
    @_sorted_keys[pos, @_sorted_keys.length].each do |ring_index|
      key = @ring[ring_index]
      next if returned_values.include?(key)
      returned_values.push(key)
    end
    
    @_sorted_keys.each_index do |i|
      break if i >= pos

      key = @ring[@_sorted_keys[i]]
      next if returned_values.include?(key)
      returned_values.push(key)      
    end

    returned_values
  end

  #
  # Given a string key this returns a long value. This long value
  # represents a location on the ring.
  #
  # MD5 is used currently.
  def gen_key(string_key)
    b_key = self._hash_digest(string_key)
    return self._hash_val(b_key) { |x| x }
  end

  #
  # Converts a hex digest to a value based on certain parts of 
  # the digest determined by the block. The block will be called
  # 4 times (with paramter 3, then 2, then 1, then 0) and is 
  # expected to return a valid index into the digest with which
  # to pull a single character from.
  #
  # This function is meant for use internally.
  def _hash_val(b_key, &block)
    return ((b_key[block.call(3)] << 24) | 
            (b_key[block.call(2)] << 16) | 
            (b_key[block.call(1)] << 8) | 
            (b_key[block.call(0)])) 
  end

  #
  # Returns raw MD5 digest of a key.
  def _hash_digest(key)
    m = Digest::MD5.new
    m.update(key)

    # No need to ord each item since ordinary array access
    # of a string in Ruby converts to ordinal value
    return m.digest
  end

  #
  # Bisects an array, returning the index where the key would
  # need to be inserted to maintain sorted order of the array.
  #
  # That being said, it is assumed that the array is already
  # in sorted order before calling this method.
  def bisect(arr, key)
    arr.each_index do |i|
      return i if key < arr[i]
    end

    return arr.length
  end
  
  def sorted_keys #:nodoc:
    @_sorted_keys
  end
end
