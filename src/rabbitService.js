#!/usr/bin/env node

const amqp = require("amqplib/callback_api");
const {queueNameToContact, rabbitMQUrl} = require('../config');

const callToUser = (mail, fullName, type) => {
  amqp.connect(rabbitMQUrl , function (error0, connection) {
    if (error0) {
      throw error0;
    }
    connection.createChannel(function (error1, channel) {
      if (error1) {
        throw error1;
      }

      channel.assertQueue(queueNameToContact, {
        durable: false,
      });
      try {
        const data = Buffer.from(JSON.stringify({ mail, fullName, type }));
        channel.sendToQueue(queueNameToContact, data);
        
      } catch (error) {
        console.log(error)
      }

      console.log(` [x] Sent details of  ${fullName} to rabbit `);
    });
  });
};


module.exports = {
  callToUser,
};