import { handler } from './index.js';  // Adjust the path if necessary

import momentTimezone from 'moment-timezone';
momentTimezone.tz.setDefault("Asia/Singapore");

const event = {
    "timestamp": "2024-09-06T03:56:50.279Z",
    "to": "2024-12-18 23:59:59", "amPm": "All", "from": "2024-12-18 00:00:00", "route": null, "driver": null, "paidBy": "All", "vehicle": null, "agencyId": 40, "selectToDate": "2024-12-18 23:59:59", "selectFromDate": "2024-12-18 00:00:00", "weekendWeekday": "All",
}

console.time("claim");
handler(event).then(response => {
    const bufferSize = response.body.length;  // Returns the size in bytes
    console.log(`Buffer size: ${bufferSize} bytes`);

    const bufferSizeInKB = response.body.length / 1024;
    console.log(`Buffer size: ${bufferSizeInKB.toFixed(2)} KB`);
}).catch(error => {
    console.error('Error:', error);
}).finally(() => {
    console.timeEnd('claim');
});

/*
function checkPunctuality(scheduledTimeStr, actualTimeStr) {
    const scheduledTimeP = momentTimezone(scheduledTimeStr);
    const actualStartTimeP = momentTimezone(actualTimeStr);

    const isPunctual = 
        actualStartTimeP.isBetween(
            scheduledTimeP.clone().subtract(10, "minutes"),
            scheduledTimeP.clone().add(5, "minutes"),
            null, '[]'
        ) || actualStartTimeP.isSame(scheduledTimeP, "minute");

    console.log(`Scheduled Time: ${scheduledTimeP.format('HH:mm:ss')}`);
    console.log(`Actual Time: ${actualStartTimeP.format('HH:mm:ss')}`);
    console.log(`Is Punctual: ${isPunctual ? 'Yes' : 'No'}`);
    console.log(`----------------------------`);
}

// Test cases
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T11:50:00'); // True (within range)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T12:04:59'); // True (within range)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T12:00:30'); // True (exact minute match)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T12:05:00'); // True (within range)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T12:05:30'); // False (outside range)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T11:49:59'); // False (outside range)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T12:06:00'); // False (outside range)
checkPunctuality('2024-08-26T12:00:00', '2024-08-26T12:05:01'); // False (outside range)
*/