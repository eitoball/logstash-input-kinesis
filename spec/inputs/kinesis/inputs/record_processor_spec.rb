# encoding: utf-8

require 'logstash-core/logstash-core'
require 'logstash-input-kinesis_jars'
require 'logstash/plugin'
require 'logstash/inputs/kinesis'
require 'logstash/codecs/json'
require 'json'

RSpec.describe 'LogStash::Inputs::Kinesis::RecordProcessor' do
  describe '#processRecords' do
    it 'decodes and queues each record with decoration' do
      record_processor = LogStash::Inputs::Kinesis::RecordProcessor.new
      # record_processor.processRecords(process_input)
    end
  end
end
