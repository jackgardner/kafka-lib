'use strict';

const _kafka = require('no-kafka');
const process = require('process');
const logger = require('nice-simple-logger');
const debug = require('debug')('kafka:lib');

module.exports = class Kafka {
  get _kafka() {
    return _kafka;
  }

  constructor(opts) {
    const options = opts || {};

    this.logger = logger({});
    this.kafkaConnectionString = options.connectionString || process.env.KAFKA_CONNECTION;
    this.kafkaClientId = options.clientId || process.env.KAFKA_CLIENT_ID;
    this.strategies = options.strategies || [];
    this.autoCommit = options.autoCommit || true;

    this.topics = this.strategies.reduce((prevStrategy, strategy) => {
      if (strategy.hasOwnProperty('subscriptions')) {
        return prevStrategy.concat(strategy.subscriptions);
      }
    }, []);
    debug(`Setting up Kafka at ${this.kafkaConnectionString}, with clientId ${this.kafkaClientId}`);
  }

  consumer(consumerOptions) {
    if (this._consumer) {
      return this._consumer;
    }

    const _consumerOpts = Object.assign({}, {
      connectionString: this.kafkaConnectionString,
      clientId: this.kafkaClientId
    }, consumerOptions);

    this._consumer = new _kafka.GroupConsumer(_consumerOpts);

    return this._consumer.init(this.strategies)
      .then(() => {
        const promises = this.topics.map(topic => this._consumer.subscribe(topic, 0));
        return Promise.all(promises);
      })
      .then(() => {
        this._consumer.on('data', (messageSet, topic, partition) => {
          messageSet.forEach(m => {
            this._consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
          });
        });

        return this._consumer;
      });
  }
};