var amqp = require('amqplib/callback_api');

function RabbitMQ(url) {
  this.url = url;
  this.connection = null;
  this.channel = null;
  this.offlinePubQueue = [];
}

RabbitMQ.prototype.closeOnErr = function(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  this.connection.close();
  this.connection = null;
  this.channel = null;
  return true;
};

RabbitMQ.prototype.connect = function() {

  var that = this;

  if (that.connection) {
    return that.connection;
  }

  that.connection = new Promise(function(resolve, reject) {
    console.log("[AMQP] connecting");

    amqp.connect(that.url + "?heartbeat=60", function(err, conn) {
      if (err) {
        console.error("[AMQP]", err.message);
        return setTimeout(that.connect, 1000);
      }
      conn.on("error", function(err) {
        if (err.message !== "Connection closing") {
          console.error("[AMQP] conn error", err.message);
        }
      });
      conn.on("close", function() {
        console.error("[AMQP] reconnecting");
        return setTimeout(that.connect, 1000);
      });

      console.log("[AMQP] connected");
      resolve(conn);
    });
  });

  return this.connection;

};

RabbitMQ.prototype.startPublisher = function() {

  var that = this;

  return this.connect().then(function(amqpConn) {

    if (that.channel) {
      return that.channel;
    }

    console.log("[AMQP] creating channel");

    that.channel = new Promise(function(resolve, reject) {
      amqpConn.createConfirmChannel(function(err, ch) {
        if (that.closeOnErr(err)) return;

        ch.on("error", function(err) {
          console.error("[AMQP] channel error", err.message);
        });

        ch.on("close", function() {
          console.log("[AMQP] channel closed");
        });

        console.log("[AMQP] channel created");
        resolve(ch);
      });
    });

    return that.channel;
  });
};

RabbitMQ.prototype.publish = function(routingKey, content) {
  exchange = 'amq.topic';

  var that = this;

  this.startPublisher().then(function(pubChannel) {

    try {
      pubChannel.publish(
        exchange,
        routingKey,
        new Buffer(JSON.stringify(content)),
        { persistent: true },
        function(err, ok) {
          if (err) {
            that.offlinePubQueue.push([routingKey, content]);
            if (that.closeOnErr(err)) return;
          }

          console.log('[AMQP] published!');
        });
    } catch (e) {
      that.offlinePubQueue.push([routingKey, content]);
      that.closeOnErr(e);
    }

  });

};

RabbitMQ.prototype.subscribe = function(queueName, cb, prefetch) {

  var that = this;

  prefetch = prefetch || 1;

  this.connect().then(function(amqpConn) {

    amqpConn.createChannel(function(err, ch) {
      if (that.closeOnErr(err)) return;

      ch.on("error", function(err) {
        console.error("[AMQP] channel error", err.message);
      });

      ch.on("close", function() {
        console.log("[AMQP] channel closed");
      });

      ch.prefetch(prefetch);
      ch.assertQueue(queueName, { durable: true }, function(err, _ok) {
        if (that.closeOnErr(err)) return;
        ch.consume(queueName, function(msg) {
          var parsedMsg = JSON.parse(msg.content.toString());

          var p = new Promise(function(resolve, reject) {
            cb(parsedMsg, resolve, reject);
          });

          p.then(function() {
            ch.ack(msg);
          });

          p.catch(function() {
            ch.reject(msg, true);
          });

        }, { noAck: false });
        console.log("Worker is started");
      });

    });

  });

};

module.exports.bus = function(url) {
  return new RabbitMQ(url);
};
