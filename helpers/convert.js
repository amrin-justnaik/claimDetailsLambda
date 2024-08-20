import crypto from 'crypto'
import { CONSTANTS } from '../config/index.js';

class Convert {
    static async jsonToCsv(data) {
        const array = typeof data != 'object' ? JSON.parse(data) : data;
        let header = '';
        let str = '';

        if (array.length === 0) return null;

        const keys = Object.keys(array[0]);

        for (let i = 0; i < keys.length; i++) {
            if (i === keys.length - 1) {
                header += `${keys[i]}`;
            } else {
                header += `${keys[i]},`;
            }
        }

        header += '\r\n'; // add new line

        for (let i = 0; i < array.length; i++) {
            let line = '';

            for (let index in array[i]) {
                if (line != '') line += ','

                line += array[i][index];
            }

            str += line + '\r\n'; // add new line
        }

        return header + str;
    }

    /**
     * Convert CSV data to JSON data
     * @param {String} data comma seperated string - CSV data
     */
    static async csvToJson(data) {
        if (typeof data !== "string") return null
        try {
            const dataBody = String(data).trim().split(/[\n]/g)
            const headers = dataBody.splice(0, 1).toString().trim().split(/[,]/g)

            const bodyJSON = []
            for (let i = 0; i < dataBody.length; i++) {
                const rowObj = {}
                const rArr = dataBody[i].trim().split(/[,]/g)

                if (headers.length !== rArr.length) return null

                headers.forEach((hCol, colNum) => {
                    const trimmed = hCol.trim()
                    rowObj[trimmed] = rArr[colNum].trim()
                })

                bodyJSON.push(rowObj)
            }

            return bodyJSON
        } catch (e) {
            return null
        }
    }

    static encodeHashedString(password) {
        const iv = crypto.randomBytes(16);
        const cipher = crypto.createCipheriv('aes-256-cbc', new Buffer.from(CONSTANTS.ENCODER_SECRET_KEY), iv);
        let encrypted = cipher.update(password);
        encrypted = new Buffer.concat([encrypted, cipher.final()]);
        return `${iv.toString('hex')}:${encrypted.toString('hex')}`;
    }

    static decodeHashedString(encodedHex) {
        const text = String(encodedHex).split(':');
        const iv = new Buffer.from(text[0], 'hex');
        const encryptedText = new Buffer.from(text[1], 'hex');
        const decipher = crypto.createDecipheriv('aes-256-cbc', new Buffer.from(CONSTANTS.ENCODER_SECRET_KEY), iv);
        let decrypted = decipher.update(encryptedText);
        decrypted = new Buffer.concat([decrypted, decipher.final()]);
        return decrypted.toString();
    }
};

export default Convert;