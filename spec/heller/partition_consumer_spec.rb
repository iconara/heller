# encoding: utf-8

require 'spec_helper'

module Heller
  describe PartitionConsumer do
    let :consumer do
      described_class.new('localhost:9092,localhost:9093', 'topic', 1337, options)
    end

    let :fake_consumer do
      double(:consumer, close: nil)
    end

    let :leader_connector do
      double(:leader_connector)
    end

    let :options do
      {leader_connector: leader_connector}
    end

    before do
      leader_connector.stub(:connect).with(['localhost:9092', 'localhost:9093'], 'topic', 1337).and_return(fake_consumer)
    end

    describe '#initialize' do
      it 'parses connect_string' do
        expect(consumer.brokers).to eq(['localhost:9092', 'localhost:9093'])
      end

      it 'sets topic' do
        expect(consumer.topic).to eq('topic')
      end

      it 'sets partition' do
        expect(consumer.partition).to eq(1337)
      end

      context 'when given an offset' do
        let :consumer do
          described_class.new('localhost:9092,localhost:9093', 'topic', 1337, offset: 128)
        end

        it 'sets offset to the one given' do
          expect(consumer.offset).to eq(128)
        end
      end

      context 'when not given an offset' do
        it 'sets offset to the magic value of earliest' do
          expect(consumer.offset).to eq(-2)
        end
      end
    end

    describe '#fetch' do
      before do
        fake_consumer.stub(:fetch)
      end

      context 'when not connected to a broker' do
        it 'connects to the leader for the topic partition combination' do
          consumer.fetch

          expect(leader_connector).to have_received(:connect).with(['localhost:9092', 'localhost:9093'], 'topic', 1337)
        end
      end

      it 'calls #fetch on connected consumer' do
        consumer.fetch

        expect(fake_consumer).to have_received(:fetch)
      end

      it 'uses given topic, partition and offset' do
        expect(fake_consumer).to receive(:fetch) do |requests|
          request = requests.first

          expect(request.topic).to eq('topic')
          expect(request.partition).to eq(1337)
          expect(request.offset).to eq(-2)
        end

        consumer.fetch
      end

      context 'when errors are raised by Kafka' do
        let :error do
          Kafka::Errors::NotLeaderForPartitionException.new
        end

        it 'retries fetching messages' do
          first_error = true
          fake_consumer.stub(:fetch) do
            if first_error
              first_error = false
              raise error
            end
          end

          consumer.fetch

          expect(fake_consumer).to have_received(:fetch).twice
        end

        it 'does :max_retries attempts and then re-raises error' do
          tries = 0
          fake_consumer.stub(:fetch) do
            if tries < 4
              tries += 1
              raise error
            end
          end

          expect { consumer.fetch }.to raise_error(error)
        end
      end
    end

    describe '#earliest_offset' do
      before do
        fake_consumer.stub(:earliest_offset).with('topic', 1337)
      end

      context 'when not connected to a broker' do
        it 'connects to the leader for the topic partition combination' do
          consumer.earliest_offset

          expect(leader_connector).to have_received(:connect).with(['localhost:9092', 'localhost:9093'], 'topic', 1337)
        end
      end

      it 'delegates #earliest_offset to connected consumer' do
        consumer.earliest_offset

        expect(fake_consumer).to have_received(:earliest_offset).with('topic', 1337)
      end

      context 'when NotLeaderForPartitionException is raised by Kafka' do
        let :error do
          Kafka::Errors::NotLeaderForPartitionException.new
        end

        it 'retries fetching earliest offset' do
          first_error = true
          fake_consumer.stub(:earliest_offset) do
            if first_error
              first_error = false
              raise error
            end
          end

          consumer.earliest_offset

          expect(fake_consumer).to have_received(:earliest_offset).twice
        end

        it 'does :max_retries attempts and then re-raises error' do
          tries = 0
          fake_consumer.stub(:earliest_offset) do
            if tries < 4
              tries += 1
              raise error
            end
          end

          expect { consumer.earliest_offset }.to raise_error(error)
        end
      end
    end

    describe '#latest_offset' do
      before do
        fake_consumer.stub(:latest_offset).with('topic', 1337)
      end

      context 'when not connected to a broker' do
        it 'connects to the leader for the topic partition combination' do
          consumer.latest_offset

          expect(leader_connector).to have_received(:connect).with(['localhost:9092', 'localhost:9093'], 'topic', 1337)
        end
      end

      it 'delegates #latest_offset to connected consumer' do
        consumer.latest_offset

        expect(fake_consumer).to have_received(:latest_offset).with('topic', 1337)
      end

      context 'when NotLeaderForPartitionException is raised by Kafka' do
        let :error do
          Kafka::Errors::NotLeaderForPartitionException.new
        end

        it 'retries fetching latest offset' do
          first_error = true
          fake_consumer.stub(:latest_offset) do
            if first_error
              first_error = false
              raise error
            end
          end

          consumer.latest_offset

          expect(fake_consumer).to have_received(:latest_offset).twice
        end

        it 'does :max_retries attempts and then re-raises error' do
          tries = 0
          fake_consumer.stub(:latest_offset) do
            if tries < 4
              tries += 1
              raise error
            end
          end

          expect { consumer.latest_offset }.to raise_error(error)
        end
      end
    end

    describe '#close' do
      context 'when connected' do
        before do
          consumer.connect
        end

        it 'closes the connected consumer' do
          consumer.close

          expect(fake_consumer).to have_received(:close)
        end
      end

      context 'when not connected' do
        it 'does nothing' do
          consumer.close

          expect(fake_consumer).not_to have_received(:close)
        end
      end
    end
  end
end
