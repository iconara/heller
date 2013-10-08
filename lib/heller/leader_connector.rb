# encoding: utf-8

module Heller
  class LeaderConnector
    def initialize(options={})
      @consumer_impl = options.delete(:consumer_impl) || Consumer
      @consumer_options = options
    end

    def connect(brokers, topic, partition)
      broker = brokers.sample
      consumer = create_consumer(broker)
      metadata = consumer.metadata([topic])
      leader = metadata.leader_for(topic, partition)

      if leader && leader.connection_string != broker
        consumer.close
        consumer = create_consumer(leader.connection_string)
      end

      consumer
    end

    private

    def create_consumer(broker)
      @consumer_impl.new(broker, @consumer_options)
    end
  end
end
