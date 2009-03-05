Gem::Specification.new do |s|
  s.specification_version = 2 if s.respond_to? :specification_version=
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
 
  s.name = 'lightcloud'
  s.version = '0.6'
  s.date = '2009-03-04'
 
  s.description = "LightCloud library for Ruby"
  s.summary = "LightCloud library for Ruby"
 
  s.authors = ["Mitchell Hashimoto"]
  s.email = "mitchell.hashimoto@gmail.com"

  s.extra_rdoc_files = %w[README.rdoc LICENSE]
  s.add_dependency 'rufus-tokyo', '>= 0.1.9'
  s.add_dependency 'mitchellh-hash_ring', '>= 0.2'

  s.has_rdoc = true
  s.homepage = 'http://github.com/mitchellh/lightcloud/'
  s.rdoc_options = ["--line-numbers", "--inline-source", "--title", "hash_ring", "--main", "README.rdoc"]
  s.require_paths = %w[lib]
  s.rubygems_version = '0.6'

  s.files = %w[
    CREDITS
    LICENSE
    README.rdoc
    Rakefile
    lib/lightcloud.rb
    spec/spec_base.rb
    spec/lightcloud_spec.rb
  ] 
end
