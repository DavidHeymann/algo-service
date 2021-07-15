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
const {delay} = require('./tools/tools');
const {patientHandler, physioHandler} = require('./tools/handler');
const {rabbitMQUrl, bucketName, folderId, queueNameFromOpenPose} = require('../config');

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
                /* const physioJsonsPathMinio = (await mongoService.getPhysioDetailsByPracticeId(videoId)).jsonPath;
                const pathInBucket = physioJsonsPathMinio.split('/').splice(-2).join('/');
                const physioFileZipPath = path.resolve(`./temp/${physioJsonsPathMinio.split('/').splice(-1).join('/')}`);
                await minioService.fetchjsonZip(bucketName, pathInBucket, physioFileZipPath);
                const dirOutputExpert = physioFileZipPath.split(".").slice(0, -1).join(".");
                const dirOutputPatient = patientFileZipPath.split(".").slice(0, -1).join(".");
                await extract(patientFileZipPath, { dir: dirOutputPatient });
                await extract(physioFileZipPath, { dir: dirOutputExpert });
                console.log("Extraction complete");
                const pyRun = util.promisify(PythonShell.run).bind(PythonShell);
                const outputPython = await pyRun("final-project/main.py", {
                  args: [
                    dirOutputExpert,
                    dirOutputPatient,
                  ],
                });
                try {
                  const jsonPath = await minioService.uploadFile(bucketName, patientFileZipPath, `jsonzips/${fileName}`);
                  const twoLineGraphPath = await minioService.uploadFile(bucketName, outputPython[0], `twolinesgraph/${path.parse(outputPython[0]).base}`);
                  const optimalGraphPath = await minioService.uploadFile(bucketName, outputPython[1], `optimalgraph/${path.parse(outputPython[1]).base}`);
                  await mongoService.updateVideoDetails(videoId, { jsonPath, optimalGraphPath, twoLineGraphPath, score: outputPython[2], status: 'success' });
                } catch (error) {
                  console.error(`An error has accourred while trying to save json files of id: ${videoId}`);
                  throw error;
                }
                fs.unlinkSync(outputPython[0]);
                fs.unlinkSync(outputPython[1]);
                rimraf.sync(dirOutputPatient);
                rimraf.sync(dirOutputExpert);
                fs.unlinkSync(physioFileZipPath);
                console.log("finished"); */
                break;
              case "physio":
                await physioHandler(videoId, fileName, patientFileZipPath);
                /* try {
                  const minioPath = await minioService.uploadFile(bucketName, patientFileZipPath, `jsonzips/${fileName}`);
                  await mongoService.updateVideoDetails(videoId, { jsonPath: minioPath, status: 'success' });
                } catch (error) {
                  console.error(`An error has accourred while trying to save json files of id: ${videoId}`);
                  throw error;
                } */
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
