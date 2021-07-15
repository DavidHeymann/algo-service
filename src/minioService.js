const Minio = require("minio");
const Fs = require('fs')
const path = require('path');

const minioBaseURL = 'http://localhost:9000'
const minioClient = new Minio.Client({
    endPoint: "127.0.0.1",
    port: 9000,
    useSSL: false,
    accessKey: "miniominio",
    secretKey: "miniominio",
});

const uploadFile = async (bucketName, filePath, pathInMinio) => {
    const fileStream = Fs.createReadStream(filePath)
    const fileStat = Fs.statSync(filePath);
    try {
        await minioClient.putObject(bucketName, pathInMinio, fileStream, fileStat.size);
        return `${minioBaseURL}/${bucketName}/${pathInMinio}`;
    } catch (error) {
        throw error;
    }
};

const fetchjsonZip = async (bucketName, pathInMinio, filePath) => {
    try {
        await minioClient.fGetObject(bucketName, pathInMinio, filePath);
        return filePath;
    } catch (error) {
        throw error;
    }
};;

module.exports = {
    uploadFile,
    fetchjsonZip,
};



