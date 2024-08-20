import fs from 'fs';
import { getGTFSFromBucket, getTripLogFromBucket, getTripLogList } from '../buckets/index.js';
import { CONSTANTS } from '../config/index.js';

class Utils {
    static buildWhereQuery(where = {}) {
        let whereQuery = '';
        const sqlParams = [];

        for (let [index, [key, value]] of Object.entries(where).entries()) {
            if (index === 0) {
                whereQuery += ` WHERE ${key} IN ($${index + 1})`;
            } else {
                whereQuery += `AND ${key} IN ($${index + 1})`;
            }
            sqlParams.push(value);
        }

        return { whereQuery, sqlParams };
    }

    static isLatitude(lat) {
        return isFinite(lat) && Math.abs(lat) <= 90;
    }

    static isLongitude(lng) {
        return isFinite(lng) && Math.abs(lng) <= 180;
    }

    static roundWalkingDistance(distance) {
        return Math.round(distance / CONSTANTS.ROUND_WALKING_DISTANCE_IN_METRE) * CONSTANTS.ROUND_WALKING_DISTANCE_IN_METRE;
    }

    static getWalkingTime(distance) {
        // calculated based on average human walking speed per metre per minute, exculding terain, descending, age, sex etc;
        return Math.ceil(distance * CONSTANTS.AVERAGE_WALKING_DISTANCE_PER_METRE);
    }

    static roundUp(value, threshold = 1) {
        return Math.ceil(value / threshold) * threshold;
    }

    static getMidNight() {
        const midnight = new Date().setHours(23, 59, 59, 999)
        return parseInt(midnight / 1000)
    }

    static async readFile(file) {
        //if not production, use local storage
        if (!CONSTANTS.IS_PRODUCTION) return fs.readFileSync(file.path, { encoding: 'utf-8' });

        try {
            const data = await getGTFSFromBucket(file.key) //expect key instead of path
            return data
        } catch (error) {
            throw error
        };
    }

    static async readTripLogFile(tripId) {
        try {
            const data = await getTripLogFromBucket(tripId)
            return data
        } catch (error) {
            throw error
        };
    }

    static getGPSLogsList(tripId) {
        // try {
            return getTripLogList(tripId)
        //     return data
        // } catch (error) {
        //     throw error
        // };
    }

    static getRandomAccessCode() {
        const result = [];
        const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

        for (let i = 0; i < 6; i++) {
            result.push(characters.charAt(Math.floor(Math.random() * characters.length)));
        }
        return result.join('');
    }
}

export default Utils;