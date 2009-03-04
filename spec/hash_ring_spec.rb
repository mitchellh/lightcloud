require File.join(File.dirname(__FILE__), 'spec_base')

# Basic constants
UNWEIGHTED_RUNS = 1000
UNWEIGHTED_ERROR_BOUND = 0.05
WEIGHTED_RUNS = 1000
WEIGHTED_ERROR_BOUND = 0.05

describe HashRing do
  include HashRingHelpers

  describe "bisection" do
    before do
      @ring = HashRing.new(['a'])
      @test_array = [10,20,30]
    end

    it "should return 0 if it less than the first element" do
      @ring.bisect(@test_array, 5).should eql(0)
    end

    it "should return the index it should go into to maintain order" do
      @ring.bisect(@test_array, 15).should eql(1)
    end

    it "should return the final index if greater than all items" do
      @ring.bisect(@test_array, 40).should eql(3)
    end
  end

  describe "iterating nodes" do
    before do
      @ring = HashRing.new(['a','b','c'])
    end

    it "should return correct values based on python" do
      a_iterate = @ring.iterate_nodes('a')
      b_iterate = @ring.iterate_nodes('b')
      c_iterate = @ring.iterate_nodes('ccccccccc')

      a_python = ["a","c","b"]
      b_python = ["b","c","a"]
      c_python = ["c","a","b"]

      (a_iterate - a_python).should be_empty
      (b_iterate - b_python).should be_empty
      (c_iterate - c_python).should be_empty
    end
  end

  describe "getting nodes" do
    def check_consistent_assigns
      first_node = @ring.get_node(@consistent_key)
      
      100.times do
        @ring.get_node(@consistent_key).should eql(first_node)
      end
    end

    
    def check_distribution
      # Keys chosen specifically from trying on Python code
      first_node = @ring.get_node('a')
      second_node = @ring.get_node('b')
      
      first_node.should_not eql(second_node)
    end

    def check_probability(run_count, error_bound, weights={})
      counts = {}
      total_counts = 0
      
      run_count.times do |i|
        node = @ring.get_node(random_string)
        
        if counts[node].nil?
          counts[node] = 0
        else
          counts[node] += 1
        end
        
        total_counts += 1
      end
      
      total_keys = counts.keys.length
      
      # Should be bounded, hopefully by 1/total_keys (give or take an error bound)
      ideal_probability = (1.0/total_keys) + error_bound
      counts.each do |node, count|
        weight = weights[node] || 1
        probability = (count / run_count.to_f)
        weighted_probability = ideal_probability * weight

        if probability >= weighted_probability
          fail "#{node} has probability: #{probability}"
        end
      end
    end

    describe "without explicit weights" do
      before do
        @ring = HashRing.new(['a','b','c'])
        @consistent_key = 'Hello, World'
      end

      it "should consistently assign nodes" do
        check_consistent_assigns
      end

      it "should distribute keys to different buckets" do
        check_distribution
      end

      it "should assign keys fairly randomly" do
        check_probability(UNWEIGHTED_RUNS, UNWEIGHTED_ERROR_BOUND)
      end
    end

    describe "with explicit weights" do
      before do
        # Create a hash ring with 'a' having a 2:1 weight
        @weights = { 'a' => 2 }
        @ring = HashRing.new(['a','b','c'], @weights)
        @consistent_key = 'Hello, World'
      end

      it "should consistently assign nodes" do
        check_consistent_assigns
      end

      it "should distribute keys to different buckets" do
        check_distribution
      end

      it "should assign keys fairly randomly, but according to weights" do
        check_probability(WEIGHTED_RUNS, WEIGHTED_ERROR_BOUND, @weights)
      end
    end
  end

  describe "hashing methods" do
    before do
      @ring = HashRing.new(['a'])
    end

    it "should return the raw digest for _hash_digest" do
      random_string = 'some random string'

      m = Digest::MD5.new
      m.update(random_string)
      
      @ring._hash_digest(random_string).should eql(m.digest)
    end

    it "should match the python output for _hash_val" do
      # This output was taken directly from the python library
      py_output = 2830561728
      ruby_output = @ring._hash_val(@ring._hash_digest('a')) { |x| x+4 }

      ruby_output.should eql(py_output)
    end
  end

  # THIS IS A VERY DIRTY WAY TO SPEC THIS
  # But given its "random" nature, I figured comparing the two libraries'
  # (one of which is in production on a huge site) output should be
  # "safe enough"
  describe "ring generation" do
    it "should generate the same ring as python, given the same inputs" do
      # Yeah... I know... terrible.
      py_output = [3747649, 3747649, 35374473, 35374473, 61840307, 61840307, 82169324, 82169324, 99513906, 99513906, 171267966, 171267966, 189092589, 189092589, 211562723, 211562723, 274168570, 274168570, 309884358, 309884358, 337859634, 337859634, 359487305, 359487305, 437877875, 437877875, 440532511, 440532511, 441427647, 441427647, 540691923, 540691923, 561744136, 561744136, 566640950, 566640950, 573631360, 573631360, 593354384, 593354384, 616375601, 616375601, 653401705, 653401705, 658933707, 658933707, 711407824, 711407824, 717967565, 717967565, 791654246, 791654246, 815230777, 815230777, 836319689, 836319689, 943387296, 943387296, 948212432, 948212432, 954761114, 954761114, 983151602, 983151602, 1041951938, 1041951938, 1044903177, 1044903177, 1109542669, 1109542669, 1215807553, 1215807553, 1234529376, 1234529376, 1240978794, 1240978794, 1241570279, 1241570279, 1245440929, 1245440929, 1295496069, 1295496069, 1359345465, 1359345465, 1371916815, 1371916815, 1440228341, 1440228341, 1463589668, 1463589668, 1542595588, 1542595588, 1571041323, 1571041323, 1580821462, 1580821462, 1609040193, 1609040193, 1663806909, 1663806909, 1673418579, 1673418579, 1725587406, 1725587406, 1743807106, 1743807106, 1745454947, 1745454947, 1770079607, 1770079607, 1816647406, 1816647406, 1823214399, 1823214399, 1858099396, 1858099396, 1889941457, 1889941457, 1903777629, 1903777629, 1956489818, 1956489818, 1981836821, 1981836821, 2027012493, 2027012493, 2036573472, 2036573472, 2063971870, 2063971870, 2113406442, 2113406442, 2203084188, 2203084188, 2245550483, 2245550483, 2369128516, 2369128516, 2401481896, 2401481896, 2405232024, 2405232024, 2439876819, 2439876819, 2498655628, 2498655628, 2666618195, 2666618195, 2709250454, 2709250454, 2725462545, 2725462545, 2761971368, 2761971368, 2820158560, 2820158560, 2847935782, 2847935782, 2873909817, 2873909817, 2960677255, 2960677255, 2970346521, 2970346521, 3065786853, 3065786853, 3173507458, 3173507458, 3187067483, 3187067483, 3189484171, 3189484171, 3196179889, 3196179889, 3200322582, 3200322582, 3234564840, 3234564840, 3262283799, 3262283799, 3310202261, 3310202261, 3326019031, 3326019031, 3332298302, 3332298302, 3347538539, 3347538539, 3365852132, 3365852132, 3378546819, 3378546819, 3430078214, 3430078214, 3453809654, 3453809654, 3467283568, 3467283568, 3469681976, 3469681976, 3494401641, 3494401641, 3522127265, 3522127265, 3523123410, 3523123410, 3555788439, 3555788439, 3585259232, 3585259232, 3587218875, 3587218875, 3587230532, 3587230532, 3627100732, 3627100732, 3642352831, 3642352831, 3670553958, 3670553958, 3721827301, 3721827301, 3746479890, 3746479890, 3836178086, 3836178086, 3887780209, 3887780209, 3927215372, 3927215372, 3953297430, 3953297430, 3967308270, 3967308270, 4025490138, 4025490138, 4045625605, 4045625605, 4094112530, 4094112530]

      ruby_output = HashRing.new(['a'])

      # Calculate the difference of the array, since ordering may be different
      (ruby_output.sorted_keys - py_output).should be_empty
    end
  end
end
