var c = require('./amqp').bus("amqp://sxevydsc:BSOzg7InZmSEz6EC3xk2civx2D4_KQjk@moose.rmq.cloudamqp.com/sxevydsc");

var x = 0;

setInterval(() => {
  console.log('producing' + (++x));
  c.publish("command.user.create", {name: 'Fulano', age: x});
}, 1000);
