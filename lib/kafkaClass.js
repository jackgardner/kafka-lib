'use strict';

const _kafka = require('no-kafka');
const process = require('process');
const logger = require('nice-simple-logger');
const debug = require('debug')('kafka:lib');

module.exports = class Kafka {
  static get _kafka() {
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

      return [];
    }, []);
    debug(`Setting up Kafka at ${this.kafkaConnectionString}, with clientId ${this.kafkaClientId}`);
  }

  producer(producerOptions) {
    if (this._producer) {
      return this._producer;
    }

    const _producerOpts = Object.assign({}, {
      connectionString: this.kafkaConnectionString,
      clientId: this.kafkaClientId
    }, producerOptions);

    this._producer = new _kafka.Producer(_producerOpts);

    return this._producer.init()
      .then(() => {
        this.logger.log('Connected to Kakfa instance');
        return this._producer;
      })
      .catch(err => {
        this.logger.error(`Error from producer: ${err}`);
      });
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
    return this._consumer.init(this.strategies);
  }
};
