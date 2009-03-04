######################################
# Tyrant Client
# Code ported from Python version written by Amir Salihefendic
######################################
# Copyright (c) 2009, Mitchell Hashimoto, mitchell.hashimoto@gmail.com
#

require 'rubygems'
require 'hash_ring'
require 'rufus/tokyo'
require 'rufus/tokyo/tyrant'

# = Tyrant Client
#
# Manages many tyrant clients and exposes basic get/set/delete
# functionality for them.
#
class TyrantClient
  @@connections = {}

  #
  # Initializes a TyrantClient. servers is expected to be an array
  # of servers in form of "host:port"
  def initialize(servers)
    @servers = servers.collect do |server|
      parts = server.split(':')
      [parts[0], parts[1].to_i]
    end
  end

  #--
  # Get/Set/Delete
  #++
  def get(key)
    db = self.get_db(key)

    db[key]
  end

  def set(key, value)
    db = self.get_db(key)

    db[key] = value
    self
  end

  def delete(key)
    db = self.get_db(key)

    begin
      db.delete(key)
      return true
    rescue
      return false
    end
  end

  #--
  # Helpers
  #++
  def get_db(key)
    index = self.hash(key) % @servers.length
    first_host, first_port = @servers[index]
    
    begin
      return self.get_connection(first_host, first_port)
    rescue
      # Didn't work, try out other servers
      @servers.each do |server|
        host, port = server

        # The python code "continues" on error code 61. Not sure
        # what that is so I'll ignore it here but TODO to go back
        # and look at it
        return self.get_connection(host, port)
      end
    end
  end

  def get_connection(host, port)
    key = "#{host}#{port}"

    return @@connections[key] unless @@connections[key].nil?

    @@connections[key] = Rufus::Tokyo::Tyrant.new(host, port)
    return @@connections[key]
  end

  def hash(key)
    m = Digest::MD5.new
    m.update(key)
    b_key = m.digest

    return ((b_key[7] << 24) | 
            (b_key[6] << 16) | 
            (b_key[5] << 8) | 
            (b_key[4])) 
  end
end

# = TyrantNode
#
# Extends TyrantClient with proper to_s functionality
class TyrantNode < TyrantClient
  #
  # Creates a TyrantNode given a name and a list of servers. The list
  # of servers should be in the format for the constructor of
  # TyrantClient
  def initialize(name, nodes)
    @name = name
    super(nodes)
  end

  def to_s
    @name
  end
end
