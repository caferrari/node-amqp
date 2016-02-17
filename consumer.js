var c = require('./amqp').bus("amqp://sxevydsc:BSOzg7InZmSEz6EC3xk2civx2D4_KQjk@moose.rmq.cloudamqp.com/sxevydsc");

c.subscribe('commands', (msg, resolve, reject) => {
  console.log('received', msg);
  resolve();
});
