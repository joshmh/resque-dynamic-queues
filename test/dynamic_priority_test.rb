dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'test/unit'
require 'rubygems'
require 'turn'
require 'resque'
require 'resque/plugins/dynamic_priority'

class DynamicPriorityTest < Test::Unit::TestCase
  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::DynamicPriority)
    end
  end

  def test_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 10
  end

  # Need to run all Resque tests after plugin is patched in

  def test_override
    Resque.pop 'bogus'
  end
end

