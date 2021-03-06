const Kafka = require('./kafkaClass');
const logger = require('nice-simple-logger')({});
module.exports = function (options) {
  'use strict';
  let _producer = null;
  new Kafka().producer(options)
    .then(producer => _producer = producer)
    .catch(() => _producer = null);

  return function (req, res, next) {
    if (!_producer) {
      logger.error('No kafka available.');
    }

    req.kafkaProducer = _producer;
    next();
  };
};
