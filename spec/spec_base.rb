# Include the hash_ring library
require File.expand_path(File.dirname(__FILE__) + '/../lib/hash_ring')

# Helpers
module HashRingHelpers
  def random_string(length=50)
    (0...length).map{ ('a'..'z').to_a[rand(26)] }.join
  end
end
