require File.join(File.dirname(__FILE__), 'spec_base')

describe LightCloud do
  before do
    @valid_servers = {
      'lookup1_A' => ['127.0.0.1:1234', '127.0.0.1:4567'],
      'storage1_A' => ['127.0.0.2:1234', '127.0.0.2:4567']
    }

    @valid_lookup_nodes, @valid_storage_nodes = LightCloud.generate_nodes(@valid_servers)

    @generic_node = mock(TyrantNode)
    [:get, :set, :delete].each do |meth|
      @generic_node.stub!(meth).and_return(nil)
    end
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
    before do
      @nodes = [mock(TyrantNode), mock(TyrantNode), mock(TyrantNode)]
      @lookup_valid_node = mock(TyrantNode)
      @storage_valid_node = mock(TyrantNode)
      
      (@nodes + [@lookup_valid_node, @storage_valid_node]).each do |node|
        node.stub!(:get).and_return(nil)
        node.stub!(:set).and_return(nil)
        node.stub!(:delete).and_return(nil)
        node.stub!(:to_s).and_return(nil)
      end

      @storage_valid_node.stub!(:to_s).and_return('storage_valid_node')
      
      @lookup_ring = mock(HashRing)
      @lookup_ring.stub!(:iterate_nodes).and_return(@nodes)
      @lookup_ring.stub!(:get_node).and_return(@lookup_valid_node)
      
      @storage_ring = mock(HashRing)
      @storage_ring.stub!(:get_node).and_return(@storage_valid_node)
      
      LightCloud.stub!(:get_lookup_ring).and_return(@lookup_ring)
      LightCloud.stub!(:get_storage_ring).and_return(@storage_ring)
      
      @key = 'foo'
      @storage_node = 'bar'
    end

    describe "locating or initting a storage node by key" do
      after do
        LightCloud.should_receive(:locate_node).with(@key, anything).once.and_return(@storage_node)
        LightCloud.locate_node_or_init(@key, LightCloud::DEFAULT_SYSTEM)
      end

      it "should just return the storage node if it was found" do
        LightCloud.should_not_receive(:get_storage_ring)
        LightCloud.should_not_receive(:get_lookup_ring)
      end

      it "should set the lookup ring to point to the new storage node if no previous storage node was found" do
        @storage_node = nil
        
        @storage_ring.should_receive(:get_node).with(@key).once.and_return(@storage_valid_node)
        @lookup_valid_node.should_receive(:set).with(@key, @storage_valid_node.to_s).once
      end
    end

    describe "locating a storage node by key" do
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

    describe "cleaning the lookup ring" do
      after do
        LightCloud._clean_up_ring(@key, @storage_node, LightCloud::DEFAULT_SYSTEM)
      end

      it "should set the key/value onto the first node (index 0)" do
        @nodes[0].should_receive(:set).with(@key, @storage_node).once
      end

      it "should delete the key from the second node (index 1)" do
        @nodes[1].should_receive(:delete).with(@key).once
      end

      it "should not touch any other nodes" do
        @nodes[2].should_not_receive(:get)
        @nodes[2].should_not_receive(:set)
        @nodes[2].should_not_receive(:delete)
      end
      
      it "should return the storage node lookup" do
        LightCloud.should_receive(:get_storage_node).with(@storage_node, anything).once
      end
    end
  end

  describe "setting" do
    before do
      @key = 'hello'
      @value = 'world!'

      LightCloud.stub!(:locate_node_or_init).and_return(@generic_node)
    end

    after do
      LightCloud.set(@key, @value)
    end

    it "should lookup the node or init for where to place key" do
      LightCloud.should_receive(:locate_node_or_init).with(@key, anything).once.and_return(@generic_node)
    end

    it "should set the value on the node returned by locate node or init" do
      @generic_node.should_receive(:set).with(@key, @value)
    end
  end
end
