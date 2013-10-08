# encoding: utf-8

module Heller
  class PartitionConsumer

    attr_reader :brokers, :topic, :partition, :offset

    def initialize(connect_string, topic, partition, options={})
      @brokers = connect_string.split(',')
      @topic, @partition = topic, partition
      @leader_connector = options.delete(:leader_connector) || LeaderConnector.new
      @offset = options.delete(:offset) || OffsetRequest.earliest_time
      @consumer = nil
      @consumer_options = options
    end

    def connect
      unless connected?
        @consumer = @leader_connector.connect(@brokers, @topic, @partition)
      end
    end

    def connected?
      !!@consumer
    end

    def fetch(options={})
      connect unless connected?

      @consumer.fetch([FetchRequest.new(@topic, @partition, @offset)])
    end

    def earliest_offset
      connect unless connected?

      @consumer.earliest_offset(@topic, @partition)
    end

    def latest_offset
      connect unless connected?

      @consumer.latest_offset(@topic, @partition)
    end

    def close
      @consumer.close if @consumer
    end
  end
end
