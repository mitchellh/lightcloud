require File.join(File.dirname(__FILE__), 'spec_base')

describe LightCloud do
  before do
    @valid_servers = {
      'lookup1_A' => ['127.0.0.1:1234', '127.0.0.1:4567'],
      'storage1_A' => ['127.0.0.2:1234', '127.0.0.2:4567']
    }

    @valid_lookup_nodes, @valid_storage_nodes = LightCloud.generate_nodes(@valid_servers)
  end

  describe "node generation" do
    it "should split lookup and storage nodes into their own arrays" do
      lookup, storage = LightCloud.generate_nodes(@valid_servers)

      lookup.should be_has_key('lookup1_A')
      storage.should be_has_key('storage1_A')
    end

    it "should ignore configuration without 'lookup' or 'storage' in it" do
      @valid_servers['foobarbaz'] = []

      lookup, storage = LightCloud.generate_nodes(@valid_servers)

      lookup.should_not be_has_key('foobarbaz')
      storage.should_not be_has_key('foobarbaz')      
    end
  end

  describe "ring generation" do
    it "should create a TyrantNode for each node" do
      TyrantNode.should_receive(:new).with('lookup1_A', anything).once
      TyrantNode.should_receive(:new).with('storage1_A', anything).once
      
      LightCloud.init(@valid_lookup_nodes, @valid_storage_nodes)
    end

    it "should return the tyrant nodes as a name to node hash" do
      unneeded, name_to_nodes = LightCloud.generate_ring(@valid_lookup_nodes)

      name_to_nodes.should be_has_key('lookup1_A')
      name_to_nodes['lookup1_A'].should be_kind_of(TyrantNode)
    end

    it "should return a hash ring with the nodes" do
      ring, unneeded = LightCloud.generate_ring(@valid_lookup_nodes)

      ring.should be_kind_of(HashRing)
    end
  end
end
