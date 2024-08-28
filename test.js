import { handler } from './index.js';  // Adjust the path if necessary

import momentTimezone from 'moment-timezone';
momentTimezone.tz.setDefault("Asia/Singapore");

// Example event
const event = {
    "from": "2024-08-24 00:00:00",
    "to": "2024-08-24 23:59:59",
    "timestamp": "1722240595",
    "route": null,
    "amPm": "All",
    "selectFromDate": "2024-08-24 00:00:00",
    "selectToDate": "2024-08-24 23:59:59",
    "vehicle": null,
    "driver": null,
    "weekendWeekday": "All",
    "paidBy": "All",
    "agencyId": 10
}

console.time("claim");
handler(event).then(response => {
    console.log(response)
}).catch(error => {
    console.error('Error:', error);
}).finally(() => {
    console.timeEnd("claim");
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