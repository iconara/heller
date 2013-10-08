# encoding: utf-8

module Heller
  class LeaderConnector
    def initialize(options={})
      @consumer_impl = options.delete(:consumer_impl) || Consumer
    end

    def connect(brokers, topic, partition, consumer_options={})
      broker = brokers.sample
      consumer = @consumer_impl.new(broker, consumer_options)
      metadata = consumer.metadata([topic])
      leader = metadata.leader_for(topic, partition)

      if leader && leader.connection_string != broker
        consumer.close
        consumer = @consumer_impl.new(leader.connection_string, consumer_options)
      end

      consumer
    end
  end
end
