import aws from 'aws-sdk';
import multer from 'multer';
import multerS3 from 'multer-s3';
import { CONSTANTS } from '../config/index.js';

aws.config.update({
    region: 'ap-southeast-1',
    accessKeyId: CONSTANTS.AWS_ACCESS_KEY_ID_1,
    secretAccessKey: CONSTANTS.AWS_ACCESS_SECRET
});

const s3 = new aws.S3();

const GTFSFileNamingConvention = (req, file, cb) => {
    const agencyId = req.user.agencyId
    const fileName = file.originalname;
    cb(null, `${agencyId}_${fileName}`)
}

const photoFileNamingConvention = (req, file, cb) => {
    const fileName = file.originalname;
    cb(null, fileName)
}

// const GTFSBucketLocal = multer({ dest: 'GTFS/' })
const GTFSBucketLocal = multer({
    storage: multer.diskStorage({
        destination: './GTFS/',
        filename: GTFSFileNamingConvention
    })
})

const GTFSBucketAWS = multer({
    storage: multerS3({
        s3: s3,
        bucket: CONSTANTS.AWS_GTFS_BUCKET,
        key: GTFSFileNamingConvention
    })
})

// const agencyPhotoBucketLocal = multer({ dest: 'uploads/' })
const agencyPhotoBucketLocal = multer({
    storage: multer.diskStorage({
        destination: './agencyPhoto/',
        filename: photoFileNamingConvention
    })
})

const agencyPhotoBucketAWS = multer({
    storage: multerS3({
        s3: s3,
        bucket: CONSTANTS.AWS_AGENCY_PHOTO_BUCKET,
        acl: 'public-read',
        key: photoFileNamingConvention,
    })
})


const getGTFSFromBucket = (Key) => {
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: CONSTANTS.AWS_GTFS_BUCKET,
            Key
        };
        s3.getObject(params, (err, data) => {
            if (err) {
                resolve([]);
            } else {
                var fileContents = data.Body.toString();
                resolve(fileContents);
            }
        });
    });
}

const getTripLogFromBucket = (tripId) => {
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: CONSTANTS.AWS_BUCKET_NAME,
            Key: `${CONSTANTS.TRIP_GPS_LOG_FILE_PREFIX}${tripId}.csv`
        };
        s3.getObject(params, (err, data) => {
            if (err) {
                resolve([]);
            } else {
                var fileContents = data.Body.toString();
                resolve(fileContents);
            }
        });
    });
}

const getTripLogList = (tripId) => {
    return new Promise((resolve) => {
        const params = {
            Bucket: CONSTANTS.AWS_BUCKET_NAME,
            Prefix: `${CONSTANTS.TRIP_GPS_LOG_FILE_PREFIX}${tripId}.csv`
        };
        s3.listObjects(params, (err, { Contents }) => {
            if (err) {
                resolve([]);
            } else {
                if (Contents.length > 0) {
                    resolve({ Key: Contents[0].Key, Size: Contents[0].Size });
                } else {
                    resolve(null)
                }
            }
        });
    });
}

const getFeedbackAttachmentUrl = async (fileName) => {
    return new Promise(async (resolve, reject) => {
        const params = {
            Bucket: CONSTANTS.AWS_ATTACHMENT_BUCKET_NAME,
            Key: fileName,
            Expires: 60 * 5 // 5 min
        };

        s3.getSignedUrl('getObject', params, (err, url) => {
            if (err) {
                return reject(err);
            }

            resolve({ url });
        });
    })
}

export { GTFSBucketAWS, GTFSBucketLocal, agencyPhotoBucketAWS, agencyPhotoBucketLocal, getGTFSFromBucket, getTripLogFromBucket, getFeedbackAttachmentUrl, getTripLogList };