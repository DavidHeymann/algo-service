const fs = require("fs");
const readline = require("readline");
const { google } = require("googleapis");
const getfilelist = require("google-drive-getfilelist");
const util = require('util');
const path = require('path');


// If modifying these scopes, delete token.json.
const SCOPES = [
  "https://www.googleapis.com/auth/drive.metadata.readonly",
  "https://www.googleapis.com/auth/drive",
  "https://www.googleapis.com/auth/drive.file",
];
// The file token.json stores the user's access and refresh tokens, and is
// created automatically when the authorization flow completes for the first
// time.
const TOKEN_PATH = "token.json";

const getFile = async (folderId, filename) => {
  const { client_secret, client_id, redirect_uris } = JSON.parse(fs.readFileSync("credentials.json")).installed;
  const oAuth2Client = new google.auth.OAuth2(
    client_id,
    client_secret,
    redirect_uris[0]
  );
  let token = ""
  try {
    token = JSON.parse(fs.readFileSync(TOKEN_PATH));
  } catch (error) {
    try {
      token = await getAccessToken(oAuth2Client);

    } catch (error) {
      console.log(error)
    }
  } finally {
    oAuth2Client.setCredentials(token);
    const fileId = await getfileId(oAuth2Client, folderId, filename);
    if (fileId) {
      const path = await getFileById(oAuth2Client, fileId, filename);
      await deleteFileById(oAuth2Client, fileId);
      console.log(`delete file ${filename} from google drive`);
      return path;
    }
    return null;
  }
};


/**
 * Get and store new token after prompting for user authorization, and then
 * execute the given callback with the authorized OAuth2 client.
 * @param {google.auth.OAuth2} oAuth2Client The OAuth2 client to get token for.
 * @param {getEventsCallback} callback The callback for the authorized client.
 */
async function getAccessToken(oAuth2Client) {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: "offline",
    scope: SCOPES,
  });
  console.log("Authorize this app by visiting this url:", authUrl);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  // Prepare readline.question for promisification
  rl.question[util.promisify.custom] = (question) => {
    return new Promise((resolve) => {
      rl.question(question, resolve);
    });
  };
  const code = await util.promisify(rl.question)('Enter the code from that page here: ');
  //   const {code, err} = await question("Enter the code from that page here: ");
  rl.close()
  const getToken = util.promisify(oAuth2Client.getToken).bind(oAuth2Client);
  const token = await getToken(code);
  // Store the token to disk for later program executions
  fs.writeFile(TOKEN_PATH, JSON.stringify(token), (err) => {
    if (err) return console.error(err);
    console.log("Token stored to", TOKEN_PATH);
  });

  return token;
}

/**
 * Using getfilelist.
 */
async function getfileId(auth, folderId, filename) {
  const resource = {
    auth: auth,
    id: folderId,
    fields: "files(name,id)",
  };
  const GetFileList = util.promisify(getfilelist.GetFileList);
  let res = await GetFileList(resource)
  let fileDetails = res.fileList[0].files.find((x) => x.name == filename);
  return fileDetails ? fileDetails.id : null;
}

const deleteFileById = async (auth, fileId) => {
  const drive = google.drive({ version: "v3", auth });
  const res = await drive.files.delete({ fileId });
};

const getFilelist = async (auth, query) => {
  if (query) {
    const drive = google.drive({ version: "v3", auth });
    const res = await drive.files.list({
      q: query,
      fields: 'files(id, name)',
      spaces: 'drive',
    })
    return res;
  } else {
    console.log(`cant bring query`);
  }
}

const getFileById = async (auth, fileId, filename) => {
  if (fileId) {
    const drive = google.drive({ version: "v3", auth });
    const res = await drive.files.get({ fileId, alt: "media" }, { responseType: "stream" });
    return new Promise((resolve, reject) => {
      const filePath = `temp/${filename}`;
      console.log(`writing to ${filePath}`);
      const dest = fs.createWriteStream(filePath);
      let progress = 0;

      res.data
        .on("end", () => {
          console.log("Done downloading file.");
          dest.end();
          resolve(path.resolve(filePath));
        })
        .on("error", (err) => {
          console.error("Error downloading file.");
          reject(err);
        })
        .pipe(dest);
    });
  } else {
    console.log(`the file ${filename} not exist`);
  }
}

module.exports = {
  getFile,
};
