require 'rake'
require 'rake/clean'
require 'rake/packagetask'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'spec/rake/spectask'
require 'fileutils'

load 'hash_ring.gemspec'

###################################
# Clean & Defaut Task
###################################
CLEAN.include('dist','tmp','rdoc')
task :default => [:clean, :repackage]

###################################
# Specs
################################### 
desc "Run all specs for hash_ring"
Spec::Rake::SpecTask.new('spec') do |t|
  t.spec_files = FileList['spec/**/*.rb']
end

###################################
# Docs
###################################
Rake::RDocTask.new do |rd|
  rd.main = 'README.rdoc'
 
  rd.rdoc_dir = 'doc'
 
  rd.rdoc_files.include(
    'README.rdoc',
    'LICENSE',
    'CREDITS',
    'lib/**/*.rb')
 
  rd.title = 'hash_ring'
 
  rd.options << '-N' # line numbers
  rd.options << '-S' # inline source
end

###################################
# Packaging - Thank you Sinatra
################################### 
# Load the gemspec using the same limitations as github
def spec
  @spec ||=
    begin
      require 'rubygems/specification'
      data = File.read('hash_ring.gemspec')
      spec = nil
      Thread.new { spec = eval("$SAFE = 3\n#{data}") }.join
      spec
    end
end
 
def package(ext='')
  "dist/hash_ring-#{spec.version}" + ext
end
 
desc 'Build packages'
task :package => %w[.gem .tar.gz].map {|e| package(e)}
 
desc 'Build and install as local gem'
task :install => package('.gem') do
  sh "gem install #{package('.gem')}"
end
 
directory 'dist/'
CLOBBER.include('dist')
 
file package('.gem') => %w[dist/ hash_ring.gemspec] + spec.files do |f|
  sh "gem build hash_ring.gemspec"
  mv File.basename(f.name), f.name
end
 
file package('.tar.gz') => %w[dist/] + spec.files do |f|
  sh <<-SH
git archive \
--prefix=hash_ring-#{source_version}/ \
--format=tar \
HEAD | gzip > #{f.name}
SH
end

def source_version
  line = File.read('lib/hash_ring.rb')[/^\s*VERSION = .*/]
  line.match(/.*VERSION = '(.*)'/)[1]
end