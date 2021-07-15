const fs = require("fs");
const { PythonShell } = require("python-shell");
const path = require('path');
const extract = require("extract-zip");
const rimraf = require("rimraf");
const util = require("util");
const minioService = require("../minioService");
const mongoService = require('../mongoService');
const { bucketName } = require('../../config');

/**
 * Compare between physio and patien 
 * @param {*} videoId 
 * @param {*} fileName 
 */
const patientHandler = async (videoId, fileName, patientFileZipPath) => {
    const physioJsonsPathMinio = (await mongoService.getPhysioDetailsByPracticeId(videoId)).jsonPath;
    const pathInBucket = physioJsonsPathMinio.split('/').splice(-2).join('/');
    const physioFileZipPath = path.resolve(`../temp/${physioJsonsPathMinio.split('/').splice(-1).join('/')}`);
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
    console.log("finished");
};

/**
 * Save jsons of physio in minio
 * @param {*} videoId 
 * @param {*} fileName 
 */
const physioHandler = async (videoId, fileName, patientFileZipPath) => {
    try {
        const minioPath = await minioService.uploadFile(bucketName, patientFileZipPath, `jsonzips/${fileName}`);
        await mongoService.updateVideoDetails(videoId, { jsonPath: minioPath, status: 'success' });
    } catch (error) {
        console.error(`An error has accourred while trying to save json files of id: ${videoId}`);
        throw error;
    }
};

module.exports = {
    patientHandler,
    physioHandler,
}