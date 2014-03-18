class Batch < Array
  
  # 
  # this doesn't appear to be used
  #
  def self.transaction
    import = new
    begin
      yield import
    rescue =>e
      import.rollback
    end
    import
  end
  def initialize(*args)
    super(*args)
    @errored = []
    @dupes = []
    @total = []
  end
  def push(*args)
    @total.push(*args)
    super(*args)
  end
  def error(item, errors)
    @total.push(item)
    @errored.push(item,errors)
  end
  def dupe(item)
    @total.push(item)
    @dupes.push(item)
  end
  def errors
    @errored
  end
  def total
    @total
  end
  def worked
    self
  end
  def rollback
    each do |i|
      next if i.destroyed? || i.new_record?
      i.destroy
    end
    clear
  end
end
