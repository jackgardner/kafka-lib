'use strict';

module.exports = {
  Kafka: require('./kafkaClass'),
  middleware: require('./express-middleware'),
  PartitionerStrategies: {
    Random: function (topic, partitions) {
      return Math.round(Math.random() * (partitions.length - 1));
    },
    AlwaysZero: function () {
      return 0;
    }
  }
};
