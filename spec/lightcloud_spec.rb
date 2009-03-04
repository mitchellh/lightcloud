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

  describe "lookup cloud methods" do
    describe "locating a storage node by key" do
      before do
        @nodes = [mock(TyrantNode), mock(TyrantNode), mock(TyrantNode)]

        @nodes.each do |node|
          node.stub!(:get).and_return(nil)
        end
        
        @lookup_ring = mock(HashRing)
        @lookup_ring.stub!(:iterate_nodes).and_return(@nodes)

        LightCloud.stub!(:get_lookup_ring).and_return(@lookup_ring)
        
        @key = 'foo'
        @storage_node = 'bar'
      end

      it "should return the storage node if the key is found in the lookup ring" do
        @nodes[0].should_receive(:get).with(@key).and_return(@storage_node)
        LightCloud.should_receive(:get_storage_node).with(@storage_node, anything).once

        LightCloud.locate_node(@key)
      end

      it "should return nil if the key doesn't exist in the lookup ring" do
        LightCloud.locate_node(@key).should be_nil
      end

      it "should attempt to clean up the lookup ring if the value is NOT found in the first node" do
        @nodes[1].should_receive(:get).with(@key).and_return(@storage_node)
        LightCloud.should_not_receive(:get_storage_node)
        LightCloud.should_receive(:_clean_up_ring).with(@key, @storage_node, anything).once

        LightCloud.locate_node(@key)
      end
    end
  end
end
