# encoding: utf-8
class LogStash::Inputs::Kinesis::RecordProcessor
  include Java::SoftwareAmazonKinesisProcessor::ShardRecordProcessor

  attr_reader(
    :checkpoint_interval,
    :codec,
    :decorator,
    :logger,
    :output_queue,
  )

  def initialize(*args)
    # nasty hack, because this is the name of a method on IRecordProcessor, but also ruby's constructor
    if !@constructed
      @codec, @output_queue, @decorator, @checkpoint_interval, @logger = args
      @next_checkpoint = Time.now - 600
      @constructed = true
    else
      _shard_id = args[0].shardId
    end
  end
  public :initialize

  def processRecords(records_input)
    records_input.records.each { |record| process_record(record) }
    if Time.now >= @next_checkpoint
      checkpoint(records_input.checkpointer)
      @next_checkpoint = Time.now + @checkpoint_interval
    end
  end

  def leaseLost(leaseLogInput)
  end

  def shardEnded(shardEndedInput)
    begin
      shardEndedInput.checkpointer().checkpoint()
    rescue Java::SoftwareAmazonKinesisException::ShutdownException, Java::SoftwareAmazonKinesisException::InvalidStateException => error
    @logger.error("Kinesis worker failed checkpointing: #{error}")
    end
  end

  def shutdownRequested(shutdownRequestedInput)
    begin
      shutdownRequestedInput.checkpointer().checkpoint()
    rescue Java::SoftwareAmazonKinesisException::ShutdownException, Java::SoftwareAmazonKinesisException::InvalidStateException => error
      @logger.error("Kinesis worker failed checkpointing: #{error}")
    end
  end

  protected

  def process_record(record)
    raw = String.from_java_bytes(record.getData.array)
    metadata = build_metadata(record)
    @codec.decode(raw) do |event|
      @decorator.call(event)
      event.set('@metadata', metadata)
      @output_queue << event
    end
  rescue => error
    @logger.error("Error processing record: #{error}")
  end

  def build_metadata(record)
    metadata = Hash.new
    metadata['approximate_arrival_timestamp'] = record.getApproximateArrivalTimestamp.getTime
    metadata['partition_key'] = record.getPartitionKey
    metadata['sequence_number'] = record.getSequenceNumber
    metadata
  end
end
