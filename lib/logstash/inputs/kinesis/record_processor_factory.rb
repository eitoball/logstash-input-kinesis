# encoding: utf-8

require 'logstash/inputs/kinesis/record_processor'

class LogStash::Inputs::Kinesis::RecordProcessorFactory
  include Java::SoftwareAmazonKinesisProcessor::ShardRecordProcessorFactory

  def shardRecordProcessor
    RecordProcessor.new
  end
end
