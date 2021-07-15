const fs = require("fs");
const { getFile } = require("./driveService");
const amqp = require("amqplib/callback_api");
const { PythonShell } = require("python-shell");
const path = require('path');
const extract = require("extract-zip");
const rimraf = require("rimraf");
const util = require("util");
const minioService = require("./minioService");
const { callToUser } = require("./rabbitService");
const mongoService = require('./mongoService');
const { delay } = require('./tools/tools');
const { patientHandler, physioHandler } = require('./tools/handler');
const { rabbitMQUrl, bucketName, folderId, queueNameFromOpenPose } = require('../config');

amqp.connect(rabbitMQUrl, function (error, connection) {
  if (error) {
    throw error;
  }
  connection.createChannel(function (error, channel) {
    if (error) {
      throw error;
    }
    const queue = queueNameFromOpenPose;

    channel.assertQueue(queue, {
      durable: false,
    });
    channel.prefetch(1);
    console.log(` [*] Waiting for messages in ${queue}. To exit press CTRL+C`);
    channel.consume(queue,
      async function (msg) {
        let { personType, zipFileName: fileName } = JSON.parse(
          msg.content.toString()
        );
        const videoId = fileName.split('_')[1].split('.')[0];
        console.log(" [x] Received %s", fileName);
        try {

          await delay(60000);
          const patientFileZipPath = await getFile(folderId, fileName);

          if (patientFileZipPath) {
            switch (personType) {
              case "patient":
                await patientHandler(videoId, fileName, patientFileZipPath)
                break;
              case "physio":
                await physioHandler(videoId, fileName, patientFileZipPath);
                break;
              default:
                console.error(`Type of person isnt recognized`);
                console.log(" [x] Done");
                return;
                break;
            }

            const user = await mongoService.getUserByVideoId(videoId);
            await callToUser(user.email, user.name, personType);
            fs.unlinkSync(patientFileZipPath);
          } else {
            console.log(`cant find file: ${fileName} in google drive!!`);
          }
          console.log(" [x] Done");
        } catch (error) {
          await mongoService.updateVideoDetails(videoId, { status: 'faild' });
        }
      },
      {
        noAck: true,
      }
    );
  });
}
);
