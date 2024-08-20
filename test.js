import { handler } from './index.js';  // Adjust the path if necessary

// Example event
const event = {
    "from": "2024-08-20 00:00:00",
    "to": "2024-08-20 23:59:59",
    "timestamp": "1722240595",
    "route": null,
    "amPm": "All",
    "selectFromDate": "2024-08-20 00:00:00",
    "selectToDate": "2024-08-20 23:59:59",
    "vehicle": null,
    "driver": null,
    "weekendWeekday": "All",
    "paidBy": "All",
    "agencyId": 34
}

handler(event).then(response => {
    console.log(response)
}).catch(error => {
    console.error('Error:', error);
});