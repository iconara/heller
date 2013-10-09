# encoding: utf-8

module Heller
  class PartitionConsumer

    attr_reader :brokers, :topic, :partition, :offset

    def initialize(connect_string, topic, partition, options={})
      @brokers = connect_string.split(',')
      @topic, @partition = topic, partition
      parse_options(options)
    end

    def connect
      connect! unless connected?
    end

    def connected?
      !!@consumer
    end

    def fetch(options={})
      with_retries do
        @consumer.fetch([FetchRequest.new(@topic, @partition, @offset)])
      end
    end

    def earliest_offset
      with_retries do
        @consumer.earliest_offset(@topic, @partition)
      end
    end

    def latest_offset
      with_retries do
        @consumer.latest_offset(@topic, @partition)
      end
    end

    def close
      @consumer.close if @consumer
    end

    private

    def parse_options(opts)
      @offset = opts.delete(:offset) || OffsetRequest.earliest_time
      @leader_connector = opts.delete(:leader_connector)
      @options = opts.delete_if { |k, _| [:max_retries, :retry_backoff].include?(k) }.merge(defaults)
      @consumer_options = opts
      @leader_connector ||= LeaderConnector.new(@consumer_options)
    end

    def defaults
      {max_retries: 3, retry_backoff: 100}
    end

    def connect!
      close
      @consumer = @leader_connector.connect(@brokers, @topic, @partition)
    end

    def with_retries
      connect unless connected?
      tries = 0

      begin
        yield
      rescue Kafka::Errors::NotLeaderForPartitionException => e
        if tries < @options[:max_retries]
          tries += 1
          Concurrency::Thread.sleep @options[:retry_backoff]
          connect!
          retry
        else
          raise
        end
      end
    end
  end
end
