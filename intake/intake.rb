require 'goliath'
class Intake < Goliath::API
  def response(env)
    [200, {}, "OOOOOOOKAY"]
  end
end
