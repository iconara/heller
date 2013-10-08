# encoding: utf-8

require 'spec_helper'

module Heller
  describe LeaderConnector do
    let :leader_connector do
      described_class.new(options)
    end

    let :consumer_impl do
      double(:consumer_impl)
    end

    let :consumer do
      double(:consumer, close: nil)
    end

    let :fake_metadata do
      double(:metadata)
    end

    let :fake_broker do
      double(:broker)
    end

    let :options do
      {consumer_impl: consumer_impl}.merge(consumer_options)
    end

    let :consumer_options do
      {timeout: 2 * 1000, client_id: 'spec-leader-lookup'}
    end

    let :brokers do
      ['localhost:9092']
    end

    let :topic do
      'topic'
    end

    let :partition do
      1337
    end

    before do
      consumer_impl.stub(:new).and_return(consumer)
      consumer.stub(:metadata).and_return(fake_metadata)
      fake_metadata.stub(:leader_for).with('topic', 1337).and_return(fake_broker)
      fake_broker.stub(:connection_string).and_return('localhost:9092')
    end

    describe '#connect' do
      it 'connects to a broker' do
        leader_connector.connect(brokers, topic, partition)

        expect(consumer_impl).to have_received(:new).with('localhost:9092', consumer_options)
      end

      it 'uses given options when creating consumer' do
        leader_connector.connect(brokers, topic, partition)

        expect(consumer_impl).to have_received(:new).with('localhost:9092', consumer_options)
      end

      it 'sends a #metadata request' do
        leader_connector.connect(brokers, topic, partition)

        expect(consumer).to have_received(:metadata).with(['topic'])
      end

      context 'if leader is different from currently connected consumer' do
        before do
          fake_broker.stub(:connection_string).and_return('localhost:9093')
        end

        it 'closes currently connected consumer' do
          leader_connector.connect(brokers, topic, partition)

          expect(consumer).to have_received(:close)
        end

        it 'connects to leader' do
          leader_connector.connect(brokers, topic, partition)

          expect(consumer_impl).to have_received(:new).with('localhost:9093', consumer_options)
        end

        it 'returns a consumer' do
          connected_consumer = leader_connector.connect(brokers, topic, partition)

          expect(connected_consumer).to eq(consumer)
        end
      end

      context 'if leader is the same as currently connected consumer' do
        it 'does not connect again' do
          leader_connector.connect(brokers, topic, partition)

          expect(consumer_impl).to have_received(:new).once
        end

        it 'returns a consumer' do
          connected_consumer = leader_connector.connect(brokers, topic, partition)

          expect(connected_consumer).to eq(consumer)
        end
      end
    end
  end
end
