import _ from "lodash";
import { Trip, Route, Stop, ScheduleV2Timetable, LambdaJobQueue } from "./models/index.js";
import { Convert, Utils } from "./helpers/index.js";
import { saveToS3 } from './buckets/bucket.js';

import momentTimezone from "moment-timezone";
import * as geolib from "geolib";
import PolylineUtils from "@mapbox/polyline";
momentTimezone.tz.setDefault("Asia/Singapore");

import zlib from 'zlib';

export const handler = async (event) => {
    // console.log("JSON.parse(JSON.stringify(event.payload))",JSON.parse(JSON.stringify(event.payload)));
    //   console.log("event",event);
    //   console.log("event.timestamp",event.timestamp);
    // Parse the payload from event.Payload
    //  const payload = JSON.parse(event.Payload);

    const {
        from,
        to,
        timestamp = null,
        route = null,
        amPm = null,
        selectFromDate = null,
        selectToDate = null,
        vehicle = null,
        driver = null,
        weekendWeekday = null,
        paidBy = null,
        agencyId = null,
        key,
        jobId
    } = event;

    // Your Lambda function logic here
    console.log(
        `from: ${from}, to: ${to}, timestamp: ${timestamp}, route: ${route}`
    );

    let tripLog = {};
    let routeStops = {};

    if (!timestamp)
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "please provide timestamp value" }),
        };

    if (!agencyId)
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "No Agency Found" }),
        };

    try {
        const jobQueue = await LambdaJobQueue.update('IN_PROGRESS', jobId);
        if (!jobQueue) {
            return {
                statusCode: 500,
                body: JSON.stringify({ message: 'Job ID not found' })
            };
        }

        const claimdata2 = await Trip.getAgencyClaimDetailsReportByDate(
            agencyId,
            timestamp,
            { from, to }
        );

        let transactionWithScheduler;
        try {
            const whereQuery = { agency_id: agencyId };

            const timetableData = await ScheduleV2Timetable.findAllDistinct(
                whereQuery
            );

            const transactionWithStartFiltered = claimdata2.filter(
                ({ startedAt, endedAt, scheduledAt }) =>
                    (momentTimezone(endedAt).diff(momentTimezone(startedAt), "minutes") >=
                        10 &&
                        momentTimezone(startedAt).isSameOrAfter(
                            momentTimezone("2022-09-17 00:00:00")
                        )) ||
                    momentTimezone(scheduledAt).isSameOrAfter(
                        momentTimezone("2022-09-17 00:00:00")
                    )
            );

            transactionWithScheduler = transactionWithStartFiltered.map((trx) => {
                if (trx.startedAt && !trx.scheduledAt) {
                    let start_time = momentTimezone(String(trx.startedAt)).format(
                        "HH:mm:ss"
                    );
                    let dayOfTrip = momentTimezone(String(trx.startedAt)).format(
                        "YYYY-MM-DD"
                    );
                    let dayOfTripName = momentTimezone(String(trx.startedAt)).format(
                        "dddd"
                    );
                    let end_time = momentTimezone(String(trx.endedAt)).format("HH:mm:ss");
                    let goal = momentTimezone(`2022-01-01 ${start_time}`).format("X");
                    let goalEnd = momentTimezone(`2022-01-01 ${end_time}`).format("X");
                    let timetableDataTemp = timetableData.filter(
                        ({ day, direction_id, route_id }) =>
                            day.toLowerCase() == dayOfTripName.toLowerCase() &&
                            direction_id == trx.obIb &&
                            route_id == trx.routeId
                    );
                    let closestStart = timetableDataTemp.reduce(function (prev, curr) {
                        let curr_time = momentTimezone(
                            `2022-01-01 ${curr.start_time}`
                        ).format("X");
                        return Math.abs(curr_time - goal) < Math.abs(prev - goal)
                            ? curr_time
                            : prev;
                    }, 0);
                    let closestEnd = timetableDataTemp.reduce(function (prev, curr) {
                        let curr_time = momentTimezone(
                            `2022-01-01 ${curr.end_time}`
                        ).format("X");
                        return Math.abs(curr_time - goalEnd) < Math.abs(prev - goalEnd)
                            ? curr_time
                            : prev;
                    }, 0);
                    const closestScheduledAt = `${dayOfTrip} ${momentTimezone
                        .unix(closestStart)
                        .format("HH:mm:ss")}`;
                    const closestScheduledEnd = momentTimezone(
                        `${dayOfTrip} ${momentTimezone.unix(closestEnd).format("HH:mm:ss")}`
                    ).format();
                    return {
                        ...trx,
                        adhoc: true,
                        scheduledAt: closestScheduledAt,
                        scheduledEndTime: closestScheduledEnd,
                    };
                } else {
                    return { ...trx };
                }
            });

            const uniqueRouteIds = [
                ...new Set(transactionWithScheduler.map((obj) => obj.routeId)),
            ];

            try {
                const { agencyId: agency_id } = event;
                if (!agency_id) {
                    return {
                        statusCode: 500,
                        body: JSON.stringify({ message: "No Agency Found" }),
                    };
                }

                const routeFetch = uniqueRouteIds.map(async (routeId) => {
                    try {
                        const route = await Route.findOne({ id: routeId, agency_id });
                        if (!route) {
                            return {
                                statusCode: 500,
                                body: JSON.stringify({ message: "Route Not Found" }),
                            };
                        }

                        const stops = (await Stop.findByRoutes(route.id)) || [];
                        route.stops = stops;
                        return route;
                    } catch (error) {
                        console.error(`Error fetching route ${routeId}:`, error);
                        throw new Error(`Error fetching route ${routeId}`);
                    }
                });

                const routeResolve = await Promise.all(routeFetch);

                routeResolve.forEach((data) => {
                    if (data?.stops?.length > 0) {
                        routeStops = {
                            ...routeStops,
                            [data.stops[0].routeId]: data.stops,
                        };
                    }
                });
            } catch (error) {
                console.error("Error in fetching routes and stops:", error);
                return {
                    statusCode: 500,
                    body: JSON.stringify({ message: "Interval Server Error" }),
                };
            }

            const uniqueTripIds = [
                ...new Set(transactionWithScheduler.map((obj) => obj.tripId)),
            ];

            const getTripLogForBulk = async (tripId) => {
                try {
                    const dataFrequency = await Utils.readTripLogFile(tripId);
                    const jsonedLog = await Convert.csvToJson(dataFrequency);
                    return jsonedLog || [];
                } catch (error) {
                    console.error(`Error fetching trip log for tripId ${tripId}:`, error);
                    return [];
                }
            };

            try {
                const { agencyId: agency_id } = event;
                if (!agency_id) {
                    return {
                        statusCode: 401,
                        body: JSON.stringify({ message: "Unauthorized: No Agency Found" }),
                    };
                }

                if (!uniqueTripIds || uniqueTripIds.length === 0) {
                    return {
                        statusCode: 400,
                        body: JSON.stringify({ message: "Please provide trip ids" }),
                    };
                }

                const tripFetch = uniqueTripIds.map((tripId) =>
                    getTripLogForBulk(tripId)
                );
                const results = await Promise.all(
                    tripFetch.map((p) => p.catch((e) => e))
                );

                const tripResolve = results.filter(
                    (result) => result.length > 0 && !(result instanceof Error)
                );
                tripResolve.forEach((data) => {
                    if (data.length > 0) {
                        tripLog = {
                            ...tripLog,
                            [data[0]?.tripId]: data,
                        };
                    }
                });
            } catch (error) {
                console.error("Error in fetching trip logs:", error);
                return {
                    statusCode: 500,
                    body: JSON.stringify({ message: "Interval Server Error" }),
                };
            }
        } catch (error) {
        } finally {
        }
        if (!transactionWithScheduler) return [];

        const filtered = transactionWithScheduler.filter(
            (
                {
                    startedAt,
                    scheduledAt,
                    routeShortName,
                    driverName,
                    vehicleRegistrationNumber,
                    userId,
                },
                index
            ) => {
                let returnVal = true;
                if (amPm !== "All") {
                    returnVal =
                        String(momentTimezone(startedAt).format("a")).toLowerCase() ===
                        String(amPm).toLowerCase();
                    if (!returnVal) return false;
                }

                if (weekendWeekday !== "All") {
                    // Adjusting the date to UTC+8
                    const adjustedDate = new Date(
                        new Date(startedAt).getTime() + 8 * 60 * 60 * 1000
                    );

                    // Checking if the adjusted date is a weekend or a weekday
                    const isWeekendWeekday = WEEKEND_DAY_NUM.includes(
                        adjustedDate.getDay()
                    )
                        ? "Weekend"
                        : "Weekday";
                    returnVal = isWeekendWeekday === weekendWeekday;
                    if (!returnVal) return false;
                }

                if (selectFromDate) {
                    returnVal = startedAt
                        ? new Date(
                            new Date(startedAt).getTime() + 8 * 60 * 60 * 1000
                        ).valueOf() >= new Date(selectFromDate).valueOf()
                        : new Date(
                            new Date(scheduledAt).getTime() + 8 * 60 * 60 * 1000
                        ).valueOf() >= new Date(selectFromDate).valueOf();
                    if (!returnVal) return false;
                }

                if (selectToDate) {
                    returnVal = startedAt
                        ? new Date(
                            new Date(startedAt).getTime() + 8 * 60 * 60 * 1000
                        ).valueOf() <= new Date(selectToDate).valueOf()
                        : new Date(
                            new Date(scheduledAt).getTime() + 8 * 60 * 60 * 1000
                        ).valueOf() <= new Date(selectToDate).valueOf();
                    if (!returnVal) return false;
                }

                if (route) {
                    returnVal = routeShortName === route;
                    if (!returnVal) return false;
                }

                if (vehicle) {
                    returnVal = vehicleRegistrationNumber === vehicle;
                    if (!returnVal) return false;
                }

                if (driver) {
                    returnVal = driverName === driver;
                    if (!returnVal) return false;
                }

                if (paidBy !== "All") {
                    returnVal = userId
                        ? "cashless"
                        : "cash" === String(paidBy).toLowerCase();
                    if (!returnVal) return false;
                }

                return true;
            }
        );
        const sortedData = _.orderBy(
            filtered,
            [
                ({ scheduledAt }) => new Date(scheduledAt),
                ({ startedAt }) => new Date(startedAt),
            ],
            ["desc", "desc"]
        );

        const addedLocalTime = sortedData?.map((d) => {
            d["localDate"] = d?.scheduledAt
                ? momentTimezone(d.scheduledAt).format("DD-MM-YYYY (ddd)")
                : d?.startedAt
                    ? momentTimezone(d.startedAt).format("DD-MM-YYYY (ddd)")
                    : "undefined";
            return d;
        });
        let addedLocalTimeOrdered = _.orderBy(addedLocalTime, ["obIb"], ["asc"]);
        const groupedData = _.groupBy(addedLocalTimeOrdered, "localDate");
        // setFilteredTripCollection(groupedData);
        // setUltraFilteredTripCollection(filtered);
        const returnData = [];
        const mainData = filtered;
        if (!mainData) return [];
        const sortedDataWithDirRoute = _.orderBy(mainData, ["routeId"], ["asc"]);
        const sortedDataWithDir = _.orderBy(
            sortedDataWithDirRoute,
            ["obIb"],
            ["asc"]
        );

        // Object.entries(mainData).forEach(([localTimeGroup, trxs]) => {
        const sortedData2 = _.orderBy(
            sortedDataWithDir,
            [
                ({ scheduledAt }) => new Date(scheduledAt),
                ({ startedAt }) => new Date(startedAt),
            ],
            ["asc", "asc"]
        );
        //

        const addedLocalTime2 = sortedData2?.map((d) => {
            d["localDate"] = d?.scheduledAt
                ? momentTimezone(d.scheduledAt).format("DD-MM-YYYY (ddd)")
                : d?.startedAt
                    ? momentTimezone(d.startedAt).format("DD-MM-YYYY (ddd)")
                    : "undefined";
            return d;
        });
        const groupedTestByRoute = _(addedLocalTime2)
            .groupBy((item) => item.routeId)
            .mapValues((routeGroup) => _.sortBy(routeGroup, "routeId"))
            .value();
        function dict_reverse(obj) {
            let new_obj = {};
            let rev_obj = Object.keys(obj).reverse();
            rev_obj.forEach(function (i) {
                new_obj[i] = obj[i];
            });
            return new_obj;
        }
        const groupedTestByRouteRev = dict_reverse(groupedTestByRoute);

        // console.log(groupedTestByRouteRev);
        Object.entries(groupedTestByRouteRev).forEach(([localTimeGroup, trxs]) => {
            const groupedData = _.groupBy(trxs, "localDate");
            //
            Object.entries(groupedData).forEach(([localTimeGroup, trxs]) => {
                const accumulativeTrip = {
                    datetime_: momentTimezone(trxs[0].startedAt).format(
                        "DD-MM-YYYY HH:mm:ss (ddd)"
                    ),
                    checkoutTime_: momentTimezone(trxs[0].endedAt).format(
                        "DD-MM-YYYY HH:mm:ss"
                    ),
                    uniqueTrip_: new Set(),
                    totalTripCount_: 0,
                    uniqueDriver_: new Set(),
                    totalUniqueDriverCount_: 0,
                    uniqueVehicle_: new Set(),
                    totalUniqueVehicleCount_: 0,
                    uniqueJourney_: new Set(),
                    totalTransaction_: 0,
                    totalAmount_: 0,
                    noOfAdult: 0,
                    noOfChild: 0,
                    noOfSenior: 0,
                    totalChild: 0,
                    totalSenior: 0,
                    totalAdult: 0,
                    noOfOku: 0,
                    noOfForeignAdult: 0,
                    noOfForeignChild: 0,
                    totalRidership_: 0,
                    cashTotalAmount_: 0,
                    cashTotalRidership_: 0,
                    cashlessTotalAmount_: 0,
                    cashlessTotalRidership_: 0,
                };
                trxs.map((row) => {
                    const totalPax =
                        row.noOfAdult +
                        +row.noOfChild +
                        +row.noOfSenior +
                        +row.noOfOku +
                        +row.noOfForeignAdult +
                        +row.noOfForeignChild;
                    accumulativeTrip["uniqueDriver_"].add(row.driverName);
                    accumulativeTrip["uniqueVehicle_"].add(row.vehicleRegistrationNumber);
                    accumulativeTrip["uniqueTrip_"].add(row.tripId);
                    accumulativeTrip["uniqueJourney_"].add(row.journeyId);
                    accumulativeTrip["totalAmount_"] += +row.amount;
                    accumulativeTrip["noOfAdult"] += +row.noOfAdult;
                    accumulativeTrip["noOfChild"] += +row.noOfChild;
                    accumulativeTrip["noOfSenior"] += +row.noOfSenior;
                    accumulativeTrip["noOfOku"] += +row.noOfOku;
                    accumulativeTrip["noOfForeignAdult"] += +row.noOfForeignAdult;
                    accumulativeTrip["noOfForeignChild"] += +row.noOfForeignChild;
                    accumulativeTrip["totalRidership_"] += totalPax;

                    accumulativeTrip["cashTotalAmount_"] += row.userId ? 0 : +row.amount;
                    accumulativeTrip["cashTotalRidership_"] += row.userId ? 0 : totalPax;

                    accumulativeTrip["cashlessTotalAmount_"] += row.userId
                        ? +row.amount
                        : 0;
                    accumulativeTrip["cashlessTotalRidership_"] += row.userId
                        ? totalPax
                        : 0;
                });

                accumulativeTrip["totalUniqueDriverCount_"] =
                    accumulativeTrip.uniqueDriver_.size;
                accumulativeTrip["totalUniqueVehicleCount_"] =
                    accumulativeTrip.uniqueVehicle_.size;
                accumulativeTrip["totalTripCount_"] = accumulativeTrip.uniqueTrip_.size;
                accumulativeTrip["totalTransaction_"] =
                    accumulativeTrip.uniqueJourney_.size;
                accumulativeTrip["totalAdult"] = accumulativeTrip.noOfAdult;
                accumulativeTrip["localTimeGroup_"] = localTimeGroup.split("+")[0];
                accumulativeTrip["trxs"] = trxs;

                //format amount
                accumulativeTrip["totalAmount_"] =
                    accumulativeTrip["totalAmount_"].toFixed(2);
                accumulativeTrip["cashTotalAmount_"] =
                    accumulativeTrip["cashTotalAmount_"].toFixed(2);
                accumulativeTrip["cashlessTotalAmount_"] =
                    accumulativeTrip["cashlessTotalAmount_"].toFixed(2);

                returnData.push(accumulativeTrip);
            });
        });

        const returnData2 = [];

        // console.log(returnData[0].trxs)
        //
        returnData.forEach(({ trxs }) => {
            const uniqueTrips = Object.values(_.groupBy(trxs, "tripId"));
            uniqueTrips.forEach((sameTripTrxs) => {
                
                //
                const totalByTrip = {
                    totalPax: 0,
                    totalAmount: 0,
                    cash: 0,
                    cashPax: 0,
                    cashless: 0,
                    cashlessPax: 0,
                    cashAdult: 0,
                    cashChild: 0,
                    cashSenior: 0,
                    cashOku: 0,
                    cashFAdult: 0,
                    cashFChild: 0,
                    cashlessAdult: 0,
                    cashlessChild: 0,
                    cashlessSenior: 0,
                    cashlessOku: 0,
                    cashlessFAdult: 0,
                    cashlessFChild: 0,
                    noOfAdult: 0,
                    noOfChild: 0,
                    noOfSenior: 0,
                    noOfOku: 0,
                    trxsTime: [],
                };
                sameTripTrxs.forEach(
                    ({
                        userId,
                        amount,
                        noOfAdult,
                        noOfChild,
                        noOfSenior,
                        noOfOku,
                        noOfForeignAdult,
                        noOfForeignChild,
                        journeyCreated,
                        journeyEnded,
                    }) => {
                        //
                        const totalPax =
                            +noOfAdult +
                            +noOfChild +
                            +noOfSenior +
                            +noOfOku +
                            +noOfForeignAdult +
                            +noOfForeignChild;
                        totalByTrip.routeId = sameTripTrxs[0].routeShortName;
                        totalByTrip.routeName = sameTripTrxs[0].routeName;
                        totalByTrip.tripId = sameTripTrxs[0].tripId;
                        totalByTrip.actualStartS = momentTimezone(
                            sameTripTrxs[0].startedAt
                        ).isValid()
                            ? momentTimezone(sameTripTrxs[0].startedAt).format("HH:mm")
                            : "-";
                        totalByTrip.actualStartFull = momentTimezone(
                            sameTripTrxs[0].startedAt
                        ).isValid()
                            ? momentTimezone(sameTripTrxs[0].startedAt)
                            : "";
                        totalByTrip.actualEnd = momentTimezone(
                            sameTripTrxs[0].endedAt
                        ).isValid()
                            ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm")
                            : "-";

                        totalByTrip.actualEndWithSeconds = momentTimezone(
                            sameTripTrxs[0].endedAt
                        ).isValid()
                            ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm:ss")
                            : "";

                        if (totalByTrip.actualEndWithSeconds == null || totalByTrip.actualEndWithSeconds == "" || totalByTrip.actualEndWithSeconds == NaN || totalByTrip.actualEndWithSeconds == undefined)
                            totalByTrip.actualEndWithSeconds = "-";

                        totalByTrip.serviceStart = momentTimezone(
                            sameTripTrxs[0].scheduledAt
                        ).isValid()
                            ? momentTimezone(sameTripTrxs[0].scheduledAt).format("HH:mm")
                            : "-";
                        totalByTrip.serviceEnd = momentTimezone(
                            sameTripTrxs[0].scheduledEndTime
                        ).isValid()
                            ? momentTimezone(sameTripTrxs[0].scheduledEndTime).format("HH:mm")
                            : "-";
                        totalByTrip.status = "No Complete";
                        totalByTrip.statusJ = "No Complete";
                        totalByTrip.statusDetail = !tripLog[sameTripTrxs[0].tripId]
                            ? "No GPS Tracking"
                            : sameTripTrxs[0].scheduledAt == null
                                ? "Trip outside schedule"
                                : "";
                        totalByTrip.busPlate = sameTripTrxs[0].vehicleRegistrationNumber;
                        totalByTrip.driverIdentification = sameTripTrxs[0].staffId;
                        totalByTrip.direction =
                            sameTripTrxs[0].obIb == 1
                                ? "OB"
                                : sameTripTrxs[0].obIb == 2
                                    ? "IB"
                                    : "LOOP";
                        // totalByTrip.totalAmount += Number(sameTripTrxs[0].amount)
                        totalByTrip.noOfAdult +=
                            Number(noOfAdult) + Number(noOfForeignAdult);
                        totalByTrip.noOfChild +=
                            Number(noOfChild) + Number(noOfForeignChild);
                        totalByTrip.noOfSenior += Number(noOfSenior);
                        totalByTrip.noOfOku += Number(noOfOku);
                        totalByTrip.cash += userId ? 0 : amount;
                        totalByTrip.cashPax += userId ? 0 : totalPax;
                        totalByTrip.cashless += userId ? amount : 0;
                        totalByTrip.totalAmount += amount;
                        totalByTrip.cashlessPax += userId ? totalPax : 0;
                        totalByTrip.cashAdult += userId ? 0 : noOfAdult;
                        totalByTrip.cashChild += userId ? 0 : noOfChild;
                        totalByTrip.cashSenior += userId ? 0 : noOfSenior;
                        totalByTrip.cashOku += userId ? 0 : noOfOku;
                        totalByTrip.cashFAdult += userId ? 0 : noOfForeignAdult;
                        totalByTrip.cashFChild += userId ? 0 : noOfForeignChild;
                        totalByTrip.cashlessAdult += userId ? noOfAdult : 0;
                        totalByTrip.cashlessChild += userId ? noOfChild : 0;
                        totalByTrip.cashlessSenior += userId ? noOfSenior : 0;
                        totalByTrip.cashlessOku += userId ? noOfOku : 0;
                        totalByTrip.cashlessFAdult += userId ? noOfForeignAdult : 0;
                        totalByTrip.cashlessFChild += userId ? noOfForeignChild : 0;
                        if (sameTripTrxs.some(t => t.adhoc)) totalByTrip.remark = 'Ad-hoc';

                        if (tripLog[sameTripTrxs[0].tripId]) {
                            const tripLogsToScan = tripLog[sameTripTrxs[0].tripId];

                            const startPoint =
                                routeStops[sameTripTrxs[0].routeId]?.filter(
                                    ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                )?.length > 0
                                    ? routeStops[sameTripTrxs[0].routeId]
                                        ?.filter(
                                            ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                        )
                                        ?.reduce(function (res, obj) {
                                            return obj.sequence < res.sequence ? obj : res;
                                        })?.name
                                    : "";

                            const startSequence =
                                routeStops[sameTripTrxs[0].routeId]?.filter(
                                    ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                )?.length > 0
                                    ? routeStops[sameTripTrxs[0].routeId]
                                        ?.filter(
                                            ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                        )
                                        ?.reduce(function (res, obj) {
                                            return obj.sequence < res.sequence ? obj : res;
                                        })?.sequence
                                    : "";

                            let timestampOfInterest = null;
                            let speedGreaterThan20Count = 0;
                            let stopNameConditionSatisfied = false;
                            let highestSequence = 0; // Initialize highestSequence

                            // Analyze the first 200 records in tripLogsToScan
                            for (let i = 0; i < Math.min(250, tripLogsToScan?.length); i++) {
                                const record = tripLogsToScan[i];

                                if (
                                    record.sequence &&
                                    record.sequence > highestSequence &&
                                    record.sequence != null &&
                                    record.sequence != "null"
                                ) {
                                    highestSequence = record.sequence;
                                }

                                if (!stopNameConditionSatisfied) {
                                    // Check the condition only if it hasn't been satisfied yet
                                    if (record.stopName.trim().toLowerCase() === startPoint.trim().toLowerCase()) {
                                        stopNameConditionSatisfied = true;
                                    }
                                }

                                if (stopNameConditionSatisfied) {
                                    // Once stopNameConditionSatisfied is true, check speed condition consecutively
                                    if (parseFloat(record.speed) >= 20) {
                                        speedGreaterThan20Count++;

                                        if (speedGreaterThan20Count === 5) {
                                            if (highestSequence == startSequence) {
                                                // If sequence has been encountered, take the timestamp of the first occurrence in history log
                                                timestampOfInterest = record.timestamp;
                                            } else if (highestSequence == startSequence + 1) {
                                                timestampOfInterest = tripLogsToScan[i - 4].timestamp;
                                            } else {
                                                timestampOfInterest = tripLogsToScan[0].timestamp;
                                            }
                                            break;
                                        }
                                    } else {
                                        // Reset the count if speed drops below 20
                                        speedGreaterThan20Count = 0;
                                    }
                                }
                            }

                            // If the timestamp is still null, take the time of the 1st occurrence
                            if (timestampOfInterest === null && tripLogsToScan.length > 0) {
                                timestampOfInterest = tripLogsToScan[0].timestamp;

                                const timestampDate =  momentTimezone(+timestampOfInterest,"x").format("DD/MM/YYYY");

                                if (timestampDate != momentTimezone(sameTripTrxs[0].startedAt).format("DD/MM/YYYY")) {
                                    console.log('time: ', timestampDate);
                                    console.log('need to search for the right date');

                                    const matchingLog = tripLogsToScan.find(log => 
                                        momentTimezone(+log.timestamp, "x").format("DD/MM/YYYY") === 
                                        momentTimezone(sameTripTrxs[0].startedAt).format("DD/MM/YYYY")
                                    );
                            
                                    if (matchingLog) {
                                        timestampOfInterest = matchingLog.timestamp;
                                        console.log('Found matching date:', momentTimezone(+matchingLog.timestamp, "x").format("DD/MM/YYYY"));
                                    }
                                }
                            }

                            totalByTrip.actualStart = momentTimezone(
                                +timestampOfInterest,
                                "x"
                            ).format("HH:mm");

                            totalByTrip.actualStartWithSeconds = momentTimezone(
                                +timestampOfInterest,
                                "x"
                            ).format("HH:mm:ss");

                            totalByTrip.actualEndWithSeconds = momentTimezone(
                                sameTripTrxs[0].endedAt
                            ).isValid()
                            ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm:ss")
                            : "";
    
                            const scheduledTimeP = momentTimezone(
                                sameTripTrxs[0].scheduledAt
                            );
                            const actualStartTimeP = momentTimezone(
                                +timestampOfInterest,
                                "x"
                            );

                            const isPunctual =
                                actualStartTimeP?.isBetween(
                                    scheduledTimeP.clone().subtract(10, "minutes"),
                                    scheduledTimeP.clone().add(5, "minutes")
                                ) || actualStartTimeP.isSame(scheduledTimeP, "minute");
                            totalByTrip.punctuality =
                                sameTripTrxs[0].scheduledAt &&
                                    sameTripTrxs[0].startedAt &&
                                    isPunctual
                                    ? "ONTIME"
                                    : "NOT PUNCTUAL";
                        } else {
                            if (totalByTrip.busPlate) {
                                totalByTrip.actualStart = momentTimezone(sameTripTrxs[0].startedAt).format("HH:mm");
                                totalByTrip.actualStartWithSeconds = momentTimezone(sameTripTrxs[0].startedAt).format("HH:mm:ss");
                                totalByTrip.actualEndWithSeconds = momentTimezone(
                                    sameTripTrxs[0].endedAt
                                ).isValid()
                                ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm:ss")
                                : "";
                            } else {
                                totalByTrip.actualStart = '-';
                                totalByTrip.actualStartWithSeconds = '-';
                                totalByTrip.actualEndWithSeconds = '-';        
                            }

                            totalByTrip.punctuality = "NOT PUNCTUAL";
                        }
                        totalByTrip.startPoint =
                            routeStops[sameTripTrxs[0].routeId]?.filter(
                                ({ directionId }) => directionId == sameTripTrxs[0].obIb
                            )?.length > 0
                                ? routeStops[sameTripTrxs[0].routeId]
                                    ?.filter(
                                        ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                    )
                                    ?.reduce(function (res, obj) {
                                        return obj.sequence < res.sequence ? obj : res;
                                    })?.name
                                : "";
                        totalByTrip.trxsTime.push(
                            userId
                                ? momentTimezone(journeyCreated).format("X")
                                : momentTimezone(journeyEnded).format("X")
                        );
                        totalByTrip.kmApad =
                            sameTripTrxs[0]?.obIb == 0
                                ? sameTripTrxs[0]?.kmLoop
                                : sameTripTrxs[0]?.obIb == 1
                                    ? sameTripTrxs[0]?.kmOutbound
                                    : sameTripTrxs[0]?.kmInbound;
                        if (sameTripTrxs[0]?.obIb == 2) {
                            if (sameTripTrxs[0]?.trip_mileage > 0) {
                                totalByTrip.kmApadG = +sameTripTrxs[0]?.trip_mileage;
                            } else {
                                totalByTrip.kmApadG = sameTripTrxs[0]?.kmInbound;
                            }
                        }
                        if (sameTripTrxs[0]?.obIb == 1) {
                            if (sameTripTrxs[0]?.trip_mileage > 0) {
                                totalByTrip.kmApadG = +sameTripTrxs[0]?.trip_mileage;
                            } else {
                                totalByTrip.kmApadG = sameTripTrxs[0]?.kmOutbound;
                            }
                        }
                        if (sameTripTrxs[0]?.obIb == 0) {
                            if (sameTripTrxs[0]?.trip_mileage > 0) {
                                totalByTrip.kmApadG = +sameTripTrxs[0]?.trip_mileage;
                            } else {
                                totalByTrip.kmApadG = sameTripTrxs[0]?.kmLoop;
                            }
                        }

                        totalByTrip.kmRate = sameTripTrxs[0]?.kmRate;
                        totalByTrip.totalClaim = 0;
                        totalByTrip.totalClaimG = 0;
                        totalByTrip.kmApadB =
                            sameTripTrxs[0]?.obIb == 0
                                ? sameTripTrxs[0]?.kmLoop
                                : sameTripTrxs[0]?.obIb == 1
                                    ? sameTripTrxs[0]?.kmOutbound
                                    : sameTripTrxs[0]?.kmInbound;
                        totalByTrip.kmRateB = sameTripTrxs[0]?.kmRate;
                        totalByTrip.monthlyPass = 0;
                        totalByTrip.jkm = 0;
                        totalByTrip.maim = 0;
                        totalByTrip.passenger = 0;
                        totalByTrip.totalOn =
                            totalByTrip.noOfAdult +
                            totalByTrip.noOfChild +
                            totalByTrip.noOfSenior +
                            totalByTrip.noOfOku;
                        totalByTrip.noOfStudent = 0;
                        totalByTrip.transferCount = 0;
                        totalByTrip.dutyId = sameTripTrxs[0]?.deviceSerialNumber;
                        totalByTrip.serviceDate = sameTripTrxs[0]?.scheduledAt
                            ? momentTimezone(sameTripTrxs[0].scheduledAt).format("DD/MM/YYYY")
                            : sameTripTrxs[0]?.startedAt
                                ? momentTimezone(sameTripTrxs[0].startedAt).format("DD/MM/YYYY")
                                : "undefined";
                        totalByTrip.busAge = sameTripTrxs[0]?.VehicleAge
                            ? momentTimezone(totalByTrip.serviceDate, "DD/MM/YYYY").year() -
                            sameTripTrxs[0]?.VehicleAge
                            : "";
                        if (
                            routeStops[sameTripTrxs[0].routeId]?.filter(
                                ({ directionId }) => directionId == sameTripTrxs[0].obIb
                            )?.length > 0
                        ) {
                            const decodedPolyline = routeStops[
                                sameTripTrxs[0].routeId
                            ]?.filter(
                                ({ directionId }) => directionId == sameTripTrxs[0].obIb
                            );
                            const arrIb = [];
                            if (
                                decodedPolyline.length > 0 &&
                                tripLog[sameTripTrxs[0].tripId]?.length > 0
                            ) {
                                decodedPolyline?.forEach((poly, index) => {
                                    //
                                    for (
                                        let index = 0;
                                        index < tripLog[sameTripTrxs[0].tripId].length;
                                        index++
                                    ) {
                                        const isNear = geolib.isPointWithinRadius(
                                            {
                                                latitude: poly.latitude,
                                                longitude: poly.longitude,
                                            },
                                            {
                                                latitude:
                                                    tripLog[sameTripTrxs[0].tripId][index].latitude,
                                                longitude:
                                                    tripLog[sameTripTrxs[0].tripId][index].longitude,
                                            },
                                            200
                                        );
                                        //
                                        if (isNear) {
                                            arrIb.push(poly.sequence);
                                            break;
                                        }
                                    }
                                });
                            }
                            const uniqueStop = [...new Set(arrIb)];
                            if (
                                sameTripTrxs[0].endedAt &&
                                (routeStops[sameTripTrxs[0].routeId]?.filter(
                                    ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                )?.length *
                                    15) /
                                100 <=
                                uniqueStop.length
                            ) {
                                totalByTrip.statusJ = "Complete";
                            }
                            totalByTrip.busStops = uniqueStop.length;
                        }
                        // for buStops travel end

                        //
                        if (sameTripTrxs[0]?.apadPolygon?.length > 0) {
                            const decodedPolyline = PolylineUtils.decode(
                                sameTripTrxs[0].apadPolygon
                            );
                            const arrIb = [];
                            const arrIbBetween = [];
                            if (
                                decodedPolyline.length > 0 &&
                                tripLog[sameTripTrxs[0].tripId]?.length > 0
                            ) {
                                decodedPolyline?.forEach((poly, index) => {
                                    if (index === 0 || index === 5) {
                                        for (
                                            let index = 0;
                                            index < tripLog[sameTripTrxs[0].tripId].length;
                                            index++
                                        ) {
                                            const isNear = geolib.isPointWithinRadius(
                                                { latitude: poly[0], longitude: poly[1] },
                                                {
                                                    latitude:
                                                        tripLog[sameTripTrxs[0].tripId][index].latitude,
                                                    longitude:
                                                        tripLog[sameTripTrxs[0].tripId][index].longitude,
                                                },
                                                200
                                            );
                                            //
                                            if (isNear) {
                                                arrIb.push(poly);
                                                break;
                                            }
                                        }
                                    }
                                });
                                for (
                                    let index = 1;
                                    index < decodedPolyline.length - 1;
                                    index++
                                ) {
                                    const element = decodedPolyline[index];
                                    for (
                                        let index = 0;
                                        index < tripLog[sameTripTrxs[0].tripId].length;
                                        index++
                                    ) {
                                        const isNear = geolib.isPointWithinRadius(
                                            { latitude: element[0], longitude: element[1] },
                                            {
                                                latitude:
                                                    tripLog[sameTripTrxs[0].tripId][index].latitude,
                                                longitude:
                                                    tripLog[sameTripTrxs[0].tripId][index].longitude,
                                            },
                                            200
                                        );
                                        if (isNear) {
                                            arrIbBetween.push(element);
                                            break;
                                        }
                                    }
                                }
                            }

                            if (arrIb?.length >= 2 && arrIbBetween?.length >= 1) {
                                totalByTrip.status = "Complete";
                            }
                        }
                    }
                );
                const numberArray = totalByTrip.trxsTime.map(Number);

                totalByTrip.salesStart = isNaN(
                    momentTimezone.unix(Math.min(...numberArray))
                )
                    ? "-"
                    : momentTimezone.unix(Math.min(...numberArray)).format("HH:mm");
                totalByTrip.salesEnd = isNaN(
                    momentTimezone.unix(Math.max(...numberArray))
                )
                    ? "-"
                    : momentTimezone.unix(Math.max(...numberArray)).format("HH:mm");

                totalByTrip.totalAmount =
                    Math.ceil(totalByTrip.totalAmount * 100) / 100;

                returnData2.push(totalByTrip);
            });
        });

        const headderWithComma = `"status of the trip (duplicate, trip outside schedule,no gps tracking, breakdown, replacement)"`;
        const headerPre =
            "\r,, ,, ,, , , ,,,,,, ,,Verified Data, ,,, , , , ,,, ,,,,ETM Boarding Passenger Count,, , ,,,,,,,,,,\r\n";
        const header = `Route No.,OD,IB/OB,Trip No.,Service Date,Start Point,RPH No.,Bus Plate Number,Bus Age,Charge/KM,Driver ID,Bus Stop Travel,Travel (KM),Total Claim,Travel (KM) GPS,Total Claim GPS,Status,${headderWithComma},KM as per BOP = ,Claim as per BOP (RM),Missed trip if no gps tracking,Start Point,Service Start Time,Actual Start Time,Sales Start Time,Service End Time,Actual End Time,Sales End Time,Punctuality,Passengers Boarding Count,Total Sales Amount (RM),Total On,Transfer Count,Monthly Pass,Adult,Child,Senior,Student,OKU,JKM,MAIM,\r\n`;

        let data = "";
        let totallTG = 0;
        let totallCTG = 0;
        let totallSTG = 0;
        let totallOTG = 0;
        let totalBusStopTG = 0;
        let totalTravelKmTG = 0;
        let totalClaimTG = 0;
        let totalClaimGpsTG = 0;
        let TravelGpsTG = 0;
        let tAmountG = 0;
        let totallTGR = 0;
        let totallCTGR = 0;
        let totallSTGR = 0;
        let totallOTGR = 0;
        let totalBusStopTGR = 0;
        let totalTravelKmTGR = 0;
        let totalClaimTGR = 0;
        let totalClaimGpsTGR = 0;
        let TravelGpsTGR = 0;
        let tAmountGR = 0;
        let grandTotalRote;
        let grandTotalRoteShort;
        let currentRoute;
        returnData.forEach(
            (
                {
                    trxs,
                    localTimeGroup_,
                    totalAmount_,
                    totalRidership_,
                    totalTripCount_,
                    cashTotalAmount_,
                    cashTotalRidership_,
                    cashlessTotalAmount_,
                    cashlessTotalRidership_,
                    totalAdult,
                },
                indexTop
            ) => {
                if (currentRoute != trxs[0].routeId && indexTop != 0) {
                    data += `, ,,,Total For Route ${grandTotalRote} : ,,,,,,,${totalBusStopTGR},${totalTravelKmTGR},${totalClaimTGR},${TravelGpsTGR},${totalClaimGpsTGR},,,,,,,,,,,,,,0,${tAmountGR},${totallTGR + totallCTGR + totallSTGR + totallOTGR
                        },0,0,${totallTGR},${totallCTGR},${totallSTGR},0,${totallOTGR},0,0\r\n`;

                    totallTGR = 0;
                    totallCTGR = 0;
                    totallSTGR = 0;
                    totallOTGR = 0;
                    totalBusStopTGR = 0;
                    totalTravelKmTGR = 0;
                    totalClaimTGR = 0;
                    totalClaimGpsTGR = 0;
                    TravelGpsTGR = 0;
                    tAmountGR = 0;
                }

                function preferredOrder(obj, order) {
                    var newObject = {};
                    for (var i = 0; i < order.length; i++) {
                        if (obj.hasOwnProperty(order[i])) {
                            newObject[order[i]] = obj[order[i]];
                        }
                    }
                    return newObject;
                }
                let tripCounter = 0;
                let tripIdCounterList = {};
                const assignedData = trxs.reverse().map((trx) => {
                    if (trx.scheduledAt && trx.scheduledEndTime) {
                        if (trx.tripId in tripIdCounterList) {
                            return { ...trx, tripNoReport: tripIdCounterList[trx.tripId] };
                        } else {
                            tripCounter++;
                            tripIdCounterList[trx.tripId] = tripCounter;
                            return { ...trx, tripNoReport: tripCounter };
                        }
                    } else {
                        return { ...trx };
                    }
                });
                const groupedTest = _.groupBy(assignedData, (item) => `"${item.obIb}"`);
                let groupedTestEdited = preferredOrder(groupedTest, [
                    '"0"',
                    '"1"',
                    '"2"',
                ]);
                let index = 0;
                let prevSchTime = "";
                let totallT = 0;
                let totallCT = 0;
                let totallST = 0;
                let totallOT = 0;
                let totalBusStopT = 0;
                let totalTravelKmT = 0;
                let totalClaimT = 0;
                let totalClaimGpsT = 0;
                let TravelGpsT = 0;
                let sDateT;
                let tAmount = 0;

                Object.entries(groupedTestEdited).forEach(([localTimeGroup, trxs]) => {
                    let totall = 0;
                    let totallC = 0;
                    let totallS = 0;
                    let totallO = 0;
                    let totalBusStop = 0;
                    let totalTravelKm = 0;
                    let totalClaim = 0;
                    let totalClaimGps = 0;
                    let TravelGps = 0;
                    let routeName;
                    let routeSName;
                    let sDate;
                    let sDateEdited;
                    let subTotal = 0;

                    const uniqueTrips = Object.values(_.groupBy(trxs, "tripId"));

                    function sort_by_key(array) {
                        return array.sort(function (a, b) {
                            var x = momentTimezone(a[0].scheduledAt).format("X");
                            var y = momentTimezone((a = b[0].scheduledAt)).format("X");
                            return x < y ? -1 : x > y ? 1 : 0;
                        });
                    }
                    const uniqueTripsOrdered = sort_by_key(uniqueTrips);

                    data += headerPre + header;
                    uniqueTripsOrdered.forEach((sameTripTrxs) => {
                        if (
                            !momentTimezone(prevSchTime).isSame(sameTripTrxs[0].scheduledAt)
                        ) {
                            index++;
                        }
                        prevSchTime = sameTripTrxs[0].scheduledAt;
                        const tripNumber =
                            sameTripTrxs[0].scheduledAt != null &&
                                sameTripTrxs[0].scheduledEndTime != null
                                ? "T" + index
                                : "";
                        const totalByTrip = {
                            totalPax: 0,
                            totalAmount: 0,
                            cash: 0,
                            cashPax: 0,
                            cashless: 0,
                            cashlessPax: 0,
                            cashAdult: 0,
                            cashChild: 0,
                            cashSenior: 0,
                            cashOku: 0,
                            cashFAdult: 0,
                            cashFChild: 0,
                            cashlessAdult: 0,
                            cashlessChild: 0,
                            cashlessSenior: 0,
                            cashlessOku: 0,
                            cashlessFAdult: 0,
                            cashlessFChild: 0,
                            noOfAdult: 0,
                            noOfChild: 0,
                            noOfSenior: 0,
                            noOfOku: 0,
                            trxsTime: [],
                        };
                        sameTripTrxs.forEach(
                            (
                                {
                                    userId,
                                    amount,
                                    noOfAdult,
                                    noOfChild,
                                    noOfSenior,
                                    noOfOku,
                                    noOfForeignAdult,
                                    noOfForeignChild,
                                    journeyCreated,
                                    journeyEnded,
                                },
                                index
                            ) => {
                                const totalPax =
                                    +noOfAdult +
                                    +noOfChild +
                                    +noOfSenior +
                                    +noOfOku +
                                    +noOfForeignAdult +
                                    +noOfForeignChild;
                                totalByTrip.routeId = sameTripTrxs[0].routeShortName;
                                totalByTrip.routeName = sameTripTrxs[0].routeName;
                                totalByTrip.tripId = sameTripTrxs[0].tripId;
                                totalByTrip.actualStartS = momentTimezone(
                                    sameTripTrxs[0].startedAt
                                ).isValid()
                                    ? momentTimezone(sameTripTrxs[0].startedAt).format("HH:mm")
                                    : "-";
                                totalByTrip.actualEnd = momentTimezone(
                                    sameTripTrxs[0].endedAt
                                ).isValid()
                                    ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm")
                                    : "-";
                                totalByTrip.serviceStart = momentTimezone(
                                    sameTripTrxs[0].scheduledAt
                                ).isValid()
                                    ? momentTimezone(sameTripTrxs[0].scheduledAt).format("HH:mm")
                                    : "-";
                                totalByTrip.serviceEnd = momentTimezone(
                                    sameTripTrxs[0].scheduledEndTime
                                ).isValid()
                                    ? momentTimezone(sameTripTrxs[0].scheduledEndTime).format(
                                        "HH:mm"
                                    )
                                    : "-";
                                totalByTrip.status = "No Complete";
                                totalByTrip.statusJ = "No Complete";
                                totalByTrip.busPlate =
                                    sameTripTrxs[0].vehicleRegistrationNumber;
                                totalByTrip.driverIdentification = sameTripTrxs[0].staffId;
                                totalByTrip.direction =
                                    sameTripTrxs[0].obIb == 1
                                        ? "OB"
                                        : sameTripTrxs[0].obIb == 2
                                            ? "IB"
                                            : "LOOP";
                                totalByTrip.noOfAdult +=
                                    Number(noOfAdult) + Number(noOfForeignAdult);
                                totalByTrip.noOfChild +=
                                    Number(noOfChild) + Number(noOfForeignChild);
                                totalByTrip.noOfSenior += Number(noOfSenior);
                                totalByTrip.noOfOku += Number(noOfOku);
                                totalByTrip.cash += userId ? 0 : amount;
                                totalByTrip.cashPax += userId ? 0 : totalPax;
                                totalByTrip.cashless += userId ? amount : 0;
                                totalByTrip.totalAmount += amount;
                                totalByTrip.cashlessPax += userId ? totalPax : 0;
                                totalByTrip.cashAdult += userId ? 0 : noOfAdult;
                                totalByTrip.cashChild += userId ? 0 : noOfChild;
                                totalByTrip.cashSenior += userId ? 0 : noOfSenior;
                                totalByTrip.cashOku += userId ? 0 : noOfOku;
                                totalByTrip.cashFAdult += userId ? 0 : noOfForeignAdult;
                                totalByTrip.cashFChild += userId ? 0 : noOfForeignChild;
                                totalByTrip.cashlessAdult += userId ? noOfAdult : 0;
                                totalByTrip.cashlessChild += userId ? noOfChild : 0;
                                totalByTrip.cashlessSenior += userId ? noOfSenior : 0;
                                totalByTrip.cashlessOku += userId ? noOfOku : 0;
                                totalByTrip.cashlessFAdult += userId ? noOfForeignAdult : 0;
                                totalByTrip.cashlessFChild += userId ? noOfForeignChild : 0;
                                totalByTrip.actualStartFull = momentTimezone(
                                    sameTripTrxs[0].startedAt
                                ).isValid()
                                    ? momentTimezone(sameTripTrxs[0].startedAt)
                                    : "";

                                if (tripLog[sameTripTrxs[0].tripId]) {
                                    const tripLogsToScan = tripLog[sameTripTrxs[0].tripId];
                                    const routeStopsData = routeStops[sameTripTrxs[0].routeId];

                                    const startPoint =
                                        routeStops[sameTripTrxs[0].routeId]?.filter(
                                            ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                        )?.length > 0
                                            ? routeStops[sameTripTrxs[0].routeId]
                                                ?.filter(
                                                    ({ directionId }) =>
                                                        directionId == sameTripTrxs[0].obIb
                                                )
                                                ?.reduce(function (res, obj) {
                                                    return obj.sequence < res.sequence ? obj : res;
                                                })?.name
                                            : "";
                                    const startSequence =
                                        routeStops[sameTripTrxs[0].routeId]?.filter(
                                            ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                        )?.length > 0
                                            ? routeStops[sameTripTrxs[0].routeId]
                                                ?.filter(
                                                    ({ directionId }) =>
                                                        directionId == sameTripTrxs[0].obIb
                                                )
                                                ?.reduce(function (res, obj) {
                                                    return obj.sequence < res.sequence ? obj : res;
                                                })?.sequence
                                            : "";

                                    let timestampOfInterest = null;
                                    let speedGreaterThan20Count = 0;
                                    let stopNameConditionSatisfied = false;
                                    let highestSequence = 0; // Initialize highestSequence

                                    for (let i = 0; i < Math.min(250, tripLogsToScan?.length); i++) {
                                        const record = tripLogsToScan[i];
                                        if (
                                            record.sequence &&
                                            record.sequence > highestSequence &&
                                            record.sequence != null &&
                                            record.sequence != "null"
                                        ) {
                                            highestSequence = record.sequence;
                                        }

                                        if (!stopNameConditionSatisfied) {
                                            if (record.stopName.trim().toLowerCase() === startPoint.trim().toLowerCase()) {
                                                stopNameConditionSatisfied = true;
                                            }
                                        }

                                        if (stopNameConditionSatisfied) {
                                            if (parseFloat(record.speed) >= 20) {
                                                speedGreaterThan20Count++;

                                                if (speedGreaterThan20Count === 5) {
                                                    if (highestSequence == startSequence) {
                                                        timestampOfInterest = record.timestamp;
                                                    } else if (highestSequence == startSequence + 1) {
                                                        timestampOfInterest =
                                                            tripLogsToScan[i - 4].timestamp;
                                                    } else {
                                                        timestampOfInterest = tripLogsToScan[0].timestamp;
                                                    }
                                                    break;
                                                }
                                            } else {
                                                speedGreaterThan20Count = 0;
                                            }
                                        }
                                    }

                                    if (timestampOfInterest === null && tripLogsToScan.length > 0) {
                                        timestampOfInterest = tripLogsToScan[0].timestamp;

                                        const timestampDate =  momentTimezone(+timestampOfInterest,"x").format("DD/MM/YYYY");

                                        if (timestampDate != momentTimezone(sameTripTrxs[0].startedAt).format("DD/MM/YYYY")) {
                                            console.log('time: ', timestampDate);
                                            console.log('need to search for the right date');

                                            const matchingLog = tripLogsToScan.find(log => 
                                                momentTimezone(+log.timestamp, "x").format("DD/MM/YYYY") === 
                                                momentTimezone(sameTripTrxs[0].startedAt).format("DD/MM/YYYY")
                                            );
                                    
                                            if (matchingLog) {
                                                timestampOfInterest = matchingLog.timestamp;
                                                console.log('Found matching date:', momentTimezone(+matchingLog.timestamp, "x").format("DD/MM/YYYY"));
                                            }
                                        }
                                    }

                                    totalByTrip.actualStart = momentTimezone(
                                        +timestampOfInterest,
                                        "x"
                                    ).format("HH:mm");

                                    totalByTrip.actualStartWithSeconds = momentTimezone(
                                        +timestampOfInterest,
                                        "x"
                                    ).format("HH:mm:ss");

                                    totalByTrip.actualEndWithSeconds = momentTimezone(
                                        sameTripTrxs[0].endedAt
                                    ).isValid()
                                    ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm:ss")
                                    : "";
            

                                    const scheduledTimeP = momentTimezone(
                                        sameTripTrxs[0].scheduledAt
                                    );
                                    const actualStartTimeP = momentTimezone(
                                        +timestampOfInterest,
                                        "x"
                                    );

                                    const isPunctual =
                                        actualStartTimeP?.isBetween(
                                            scheduledTimeP.clone().subtract(10, "minutes"),
                                            scheduledTimeP.clone().add(5, "minutes")
                                        ) || actualStartTimeP.isSame(scheduledTimeP, "minute");

                                    totalByTrip.punctuality =
                                        sameTripTrxs[0].scheduledAt &&
                                            sameTripTrxs[0].startedAt &&
                                            isPunctual
                                            ? "ONTIME"
                                            : "NOT PUNCTUAL";
                                } else {
                                    if (totalByTrip.busPlate) {
                                        totalByTrip.actualStart = momentTimezone(sameTripTrxs[0].startedAt).format("HH:mm");
                                        totalByTrip.actualStartWithSeconds = momentTimezone(sameTripTrxs[0].startedAt).format("HH:mm:ss");
                                        totalByTrip.actualEndWithSeconds = momentTimezone(
                                            sameTripTrxs[0].endedAt
                                        ).isValid()
                                        ? momentTimezone(sameTripTrxs[0].endedAt).format("HH:mm:ss")
                                        : "";
                                    } else {
                                        totalByTrip.actualStart = '-';
                                        totalByTrip.actualStartWithSeconds = '-';
                                        totalByTrip.actualEndWithSeconds = '-';
                                    }

                                    if (sameTripTrxs[0].adhoc) totalByTrip.remark = 'Ad-hoc';
                                    totalByTrip.punctuality = "NOT PUNCTUAL";
                                }

                                totalByTrip.startPoint =
                                    routeStops[sameTripTrxs[0].routeId]?.filter(
                                        ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                    )?.length > 0
                                        ? routeStops[sameTripTrxs[0].routeId]
                                            ?.filter(
                                                ({ directionId }) =>
                                                    directionId == sameTripTrxs[0].obIb
                                            )
                                            ?.reduce(function (res, obj) {
                                                return obj.sequence < res.sequence ? obj : res;
                                            })?.name
                                        : "";
                                totalByTrip.trxsTime.push(
                                    userId
                                        ? momentTimezone(journeyCreated).format("X")
                                        : momentTimezone(journeyEnded).format("X")
                                );

                                totalByTrip.totalClaim = 0;
                                totalByTrip.monthlyPass = 0;
                                totalByTrip.jkm = 0;
                                totalByTrip.maim = 0;
                                totalByTrip.passenger = 0;
                                totalByTrip.totalOn =
                                    totalByTrip.noOfAdult +
                                    totalByTrip.noOfChild +
                                    totalByTrip.noOfSenior +
                                    totalByTrip.noOfOku;
                                totalByTrip.noOfStudent = 0;
                                totalByTrip.transferCount = 0;
                                totalByTrip.rph = sameTripTrxs[0]?.deviceSerialNumber;
                                totalByTrip.serviceDate = sameTripTrxs[0]?.scheduledAt
                                    ? momentTimezone(sameTripTrxs[0].scheduledAt).format(
                                        "DD/MM/YYYY"
                                    )
                                    : sameTripTrxs[0]?.startedAt
                                        ? momentTimezone(sameTripTrxs[0].startedAt).format(
                                            "DD/MM/YYYY"
                                        )
                                        : "undefined";
                                totalByTrip.busAge = sameTripTrxs[0]?.VehicleAge
                                    ? momentTimezone(
                                        totalByTrip.serviceDate,
                                        "DD/MM/YYYY"
                                    ).year() - sameTripTrxs[0]?.VehicleAge
                                    : "";
                                totalByTrip.serviceDateEdited = sameTripTrxs[0]?.scheduledAt
                                    ? momentTimezone(sameTripTrxs[0].scheduledAt).format(
                                        "DD/MM/YYYY"
                                    )
                                    : sameTripTrxs[0]?.startedAt
                                        ? momentTimezone(sameTripTrxs[0].startedAt).format(
                                            "DD/MM/YYYY"
                                        )
                                        : "undefined";
                                totalByTrip.kmApad =
                                    sameTripTrxs[0]?.obIb == 0
                                        ? sameTripTrxs[0]?.kmLoop
                                        : sameTripTrxs[0]?.obIb == 1
                                            ? sameTripTrxs[0]?.kmOutbound
                                            : sameTripTrxs[0]?.kmInbound;
                                if (sameTripTrxs[0]?.obIb == 2) {
                                    if (sameTripTrxs[0]?.trip_mileage > 0) {
                                        totalByTrip.kmApadG = +sameTripTrxs[0]?.trip_mileage;
                                    } else {
                                        totalByTrip.kmApadG = sameTripTrxs[0]?.kmInbound;
                                    }
                                }
                                if (sameTripTrxs[0]?.obIb == 1) {
                                    if (sameTripTrxs[0]?.trip_mileage > 0) {
                                        totalByTrip.kmApadG = +sameTripTrxs[0]?.trip_mileage;
                                    } else {
                                        totalByTrip.kmApadG = sameTripTrxs[0]?.kmOutbound;
                                    }
                                }
                                if (sameTripTrxs[0]?.obIb == 0) {
                                    if (sameTripTrxs[0]?.trip_mileage > 0) {
                                        totalByTrip.kmApadG = +sameTripTrxs[0]?.trip_mileage;
                                    } else {
                                        totalByTrip.kmApadG = sameTripTrxs[0]?.kmLoop;
                                    }
                                }
                                totalByTrip.kmRate = sameTripTrxs[0]?.kmRate;
                                totalByTrip.totalClaim = 0;
                                totalByTrip.totalClaimG = 0;
                                totalByTrip.kmApadB =
                                    sameTripTrxs[0]?.obIb == 0
                                        ? sameTripTrxs[0]?.kmLoop
                                        : sameTripTrxs[0]?.obIb == 1
                                            ? sameTripTrxs[0]?.kmOutbound
                                            : sameTripTrxs[0]?.kmInbound;
                                totalByTrip.kmRateB = sameTripTrxs[0]?.kmRate;
                                totalByTrip.statusDetail = !tripLog[sameTripTrxs[0].tripId]
                                    ? "No GPS Tracking"
                                    : sameTripTrxs[0].scheduledAt == null
                                        ? "Trip outside schedule"
                                        : "";

                                // for buStops travel start
                                if (
                                    routeStops[sameTripTrxs[0].routeId]?.filter(
                                        ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                    )?.length > 0
                                ) {
                                    const decodedPolyline = routeStops[
                                        sameTripTrxs[0].routeId
                                    ]?.filter(
                                        ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                    );
                                    const arrIb = [];
                                    if (
                                        decodedPolyline.length > 0 &&
                                        tripLog[sameTripTrxs[0].tripId]?.length > 0
                                    ) {
                                        decodedPolyline?.forEach((poly, index) => {
                                            //
                                            for (
                                                let index = 0;
                                                index < tripLog[sameTripTrxs[0].tripId].length;
                                                index++
                                            ) {
                                                const isNear = geolib.isPointWithinRadius(
                                                    {
                                                        latitude: poly.latitude,
                                                        longitude: poly.longitude,
                                                    },
                                                    {
                                                        latitude:
                                                            tripLog[sameTripTrxs[0].tripId][index].latitude,
                                                        longitude:
                                                            tripLog[sameTripTrxs[0].tripId][index].longitude,
                                                    },
                                                    200
                                                );
                                                if (isNear) {
                                                    arrIb.push(poly.sequence);
                                                    break;
                                                }
                                            }
                                        });
                                    }
                                    const uniqueStop = [...new Set(arrIb)];
                                    if (
                                        sameTripTrxs[0].endedAt &&
                                        (routeStops[sameTripTrxs[0].routeId]?.filter(
                                            ({ directionId }) => directionId == sameTripTrxs[0].obIb
                                        )?.length *
                                            15) /
                                        100 <=
                                        uniqueStop.length
                                    ) {
                                        totalByTrip.statusJ = "Complete";
                                    }
                                    totalByTrip.busStops = uniqueStop.length;
                                }

                                if (sameTripTrxs[0]?.apadPolygon?.length > 0) {
                                    const decodedPolyline = PolylineUtils.decode(
                                        sameTripTrxs[0].apadPolygon
                                    );
                                    const arrIb = [];
                                    const arrIbBetween = [];
                                    if (
                                        decodedPolyline.length > 0 &&
                                        tripLog[sameTripTrxs[0].tripId]?.length > 0
                                    ) {
                                        decodedPolyline?.forEach((poly, index) => {
                                            if (index === 0 || index === 5) {
                                                for (
                                                    let index = 0;
                                                    index < tripLog[sameTripTrxs[0].tripId].length;
                                                    index++
                                                ) {
                                                    const isNear = geolib.isPointWithinRadius(
                                                        { latitude: poly[0], longitude: poly[1] },
                                                        {
                                                            latitude:
                                                                tripLog[sameTripTrxs[0].tripId][index].latitude,
                                                            longitude:
                                                                tripLog[sameTripTrxs[0].tripId][index]
                                                                    .longitude,
                                                        },
                                                        200
                                                    );
                                                    if (isNear) {
                                                        arrIb.push(poly);
                                                        break;
                                                    }
                                                }
                                            }
                                        });
                                        for (
                                            let index = 1;
                                            index < decodedPolyline.length - 1;
                                            index++
                                        ) {
                                            const element = decodedPolyline[index];
                                            for (
                                                let index = 0;
                                                index < tripLog[sameTripTrxs[0].tripId].length;
                                                index++
                                            ) {
                                                const isNear = geolib.isPointWithinRadius(
                                                    { latitude: element[0], longitude: element[1] },
                                                    {
                                                        latitude:
                                                            tripLog[sameTripTrxs[0].tripId][index].latitude,
                                                        longitude:
                                                            tripLog[sameTripTrxs[0].tripId][index].longitude,
                                                    },
                                                    200
                                                );
                                                //
                                                if (isNear) {
                                                    // check if inbound and scheduled?
                                                    arrIbBetween.push(element);
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    if (arrIb?.length >= 2 && arrIbBetween?.length >= 1) {
                                        totalByTrip.status = "Complete";
                                    }
                                }
                            }
                        );

                        const numberArray = totalByTrip.trxsTime.map(Number);
                        grandTotalRoteShort = `${sameTripTrxs[0].routeShortName}`;
                        grandTotalRote = `${sameTripTrxs[0].routeShortName} ${sameTripTrxs[0].routeName}`;
                        totall = Number(totall) + Number(totalByTrip.noOfAdult);
                        totallC = Number(totallC) + Number(totalByTrip.noOfChild);
                        totallS = Number(totallS) + Number(totalByTrip.noOfSenior);
                        totallO = Number(totallO) + Number(totalByTrip.noOfOku);
                        totalByTrip.salesStart = isNaN(
                            momentTimezone.unix(Math.min(...numberArray))
                        )
                            ? "-"
                            : momentTimezone.unix(Math.min(...numberArray)).format("HH:mm");
                        totalByTrip.salesEnd = isNaN(
                            momentTimezone.unix(Math.max(...numberArray))
                        )
                            ? "-"
                            : momentTimezone.unix(Math.max(...numberArray)).format("HH:mm");
                        totalBusStop = Number(totalBusStop) + Number(totalByTrip.busStops);
                        totalTravelKm = Number(totalTravelKm) + Number(totalByTrip.kmApad);
                        totalClaim = Number(totalClaim) + Number(totalByTrip.totalClaim);
                        totalClaimGps =
                            Number(totalClaimGps) + Number(totalByTrip.totalClaimG);
                        TravelGps = Number(TravelGps) + Number(totalByTrip.kmApadG);
                        routeName = totalByTrip.routeName;
                        routeSName = totalByTrip.routeId;
                        sDate = totalByTrip.serviceDate;
                        // returnData.push(totalByTrip);
                        sDateT = totalByTrip.serviceDateEdited;
                        sDateEdited = `"${totalByTrip.serviceDateEdited} "`;

                        if (totalByTrip.actualEndWithSeconds == null || totalByTrip.actualEndWithSeconds == "" || totalByTrip.actualEndWithSeconds == NaN || totalByTrip.actualEndWithSeconds == undefined)
                            totalByTrip.actualEndWithSeconds = "-";

                        data += `${sameTripTrxs[0].routeShortName},${sameTripTrxs[0].routeShortName
                            } ${sameTripTrxs[0].routeName},${totalByTrip.direction
                            },${tripNumber},${sDateEdited} ,${totalByTrip.startPoint},${sameTripTrxs[0].tripId
                            },${totalByTrip.busPlate},${totalByTrip.busAge},${totalByTrip.kmRate
                            },${totalByTrip.driverIdentification},${totalByTrip.busStops
                            },${Number(totalByTrip.kmApad).toFixed(2)},${totalByTrip.totalClaim
                            },${Number(totalByTrip.kmApadG).toFixed(2)},${totalByTrip.totalClaimG
                            },${totalByTrip.status},${totalByTrip.statusDetail},${totalByTrip.kmApadB
                            },${totalByTrip.kmRateB},,${totalByTrip.actualStartS},${totalByTrip.serviceStart
                            },${totalByTrip.actualStartWithSeconds},${totalByTrip.salesStart},${totalByTrip.serviceEnd
                            },${totalByTrip.actualEndWithSeconds},${totalByTrip.salesEnd},${totalByTrip.punctuality
                            }, ${totalByTrip.passenger},${totalByTrip.totalAmount.toFixed(2)},${totalByTrip.totalOn
                            },${totalByTrip.transferCount},${totalByTrip.monthlyPass},${totalByTrip.noOfAdult
                            },${totalByTrip.noOfChild},${totalByTrip.noOfSenior} ,${totalByTrip.noOfStudent
                            },${totalByTrip.noOfOku},${totalByTrip.jkm}, ${totalByTrip.maim
                            },\r`;
                        subTotal = subTotal + totalByTrip.totalAmount;
                    });
                    //
                    data += `, ,,,Total (${sDateT} - ${routeSName} ${routeName}),,,,,,,${totalBusStop},${totalTravelKm.toFixed(
                        2
                    )},${totalClaim},${TravelGps.toFixed(
                        2
                    )},${totalClaimGps},,,,,,,,,,,,,,0,${subTotal.toFixed(2)},${totall + totallC + totallS + totallO
                        },0,0,${totall},${totallC},${totallS},0,${totallO},0,0\r\n`;
                    index = 0;
                    totallT = Number(totallT) + Number(totall);
                    totallCT = Number(totallCT) + Number(totallC);
                    totallST = Number(totallST) + Number(totallS);
                    totallOT = Number(totallOT) + Number(totallO);
                    totalBusStopT = Number(totalBusStopT) + Number(totalBusStop);
                    totalTravelKmT = Number(totalTravelKmT) + Number(totalTravelKm);
                    totalClaimT = Number(totalClaimT) + Number(totalClaim);
                    totalClaimGpsT = Number(totalClaimGpsT) + Number(totalClaimGps);
                    TravelGpsT = Number(TravelGpsT) + Number(TravelGps);
                    tAmount = Number(tAmount) + Number(subTotal);
                });

                totallTG = totallTG + totallT;
                totallCTG = totallCTG + totallCT;
                totallSTG = totallSTG + totallST;
                totallOTG = totallOTG + totallOT;
                totalBusStopTG = totalBusStopTG + totalBusStopT;
                totalTravelKmTG = totalTravelKmTG + totalTravelKmT;
                totalClaimTG = totalClaimTG + totalClaimT;
                totalClaimGpsTG = totalClaimGpsTG + totalClaimGpsT;
                TravelGpsTG = TravelGpsTG + TravelGpsT;
                tAmountG = tAmountG + tAmount;

                totallTGR = totallTGR + totallT;
                totallCTGR = totallCTGR + totallCT;
                totallSTGR = totallSTGR + totallST;
                totallOTGR = totallOTGR + totallOT;
                totalBusStopTGR = totalBusStopTGR + totalBusStopT;
                totalTravelKmTGR = totalTravelKmTGR + totalTravelKmT;
                totalClaimTGR = totalClaimTGR + totalClaimT;
                totalClaimGpsTGR = totalClaimGpsTGR + totalClaimGpsT;
                TravelGpsTGR = TravelGpsTGR + TravelGpsT;
                tAmountGR = tAmountGR + tAmount;
                data += `, ,,,Total For Service Date : ${sDateT} ,,,,,,,${totalBusStopT},${totalTravelKmT.toFixed(
                    2
                )},${totalClaimT},${TravelGpsT.toFixed(
                    2
                )},${totalClaimGpsT},,,,,,,,,,,,,,0,${tAmount.toFixed(2)},${totallT + totallCT + totallST + totallOT
                    },0,0,${totallT},${totallCT},${totallST},0,${totallOT},0,0\r\n`;
                if (indexTop == returnData.length - 1) {
                    data += `, ,,,Total For Route ${grandTotalRote} : ,,,,,,,${totalBusStopTGR},${totalTravelKmTGR.toFixed(
                        2
                    )},${totalClaimTGR},${TravelGpsTGR.toFixed(
                        2
                    )},${totalClaimGpsTGR},,,,,,,,,,,,,,0,${tAmountGR.toFixed(2)},${totallTGR + totallCTGR + totallSTGR + totallOTGR
                        },0,0,${totallTGR},${totallCTGR},${totallSTGR},0,${totallOTGR},0,0\r\n`;

                    totallTGR = 0;
                    totallCTGR = 0;
                    totallSTGR = 0;
                    totallOTGR = 0;
                    totalBusStopTGR = 0;
                    totalTravelKmTGR = 0;
                    totalClaimTGR = 0;
                    totalClaimGpsTGR = 0;
                    TravelGpsTGR = 0;
                    tAmountGR = 0;
                }
                currentRoute = trxs[0].routeId;
            }
        );

        // data += `, ,,,Total For Route ${grandTotalRote} : ,,,,,,,${totalBusStopTG},${totalTravelKmTG},${totalClaimTG},${TravelGpsTG},${totalClaimGpsTG},,,,,,,,,,,,,,0,${tAmountG},${totallTG + totallCTG + totallSTG + totallOTG},0,0,${totallTG},${totallCTG},${totallSTG},0,${totallOTG},0,0\r\n`
        data += `, ,,,Grand Total :,,,,,,,${totalBusStopTG},${totalTravelKmTG.toFixed(
            2
        )},${totalClaimTG},${TravelGpsTG.toFixed(
            2
        )},${totalClaimGpsTG},,,,,,,,,,,,,,0,${tAmountG.toFixed(2)},${totallTG + totallCTG + totallSTG + totallOTG
            },0,0,${totallTG},${totallCTG},${totallSTG},0,${totallOTG},0,0\r\n`;
        // data += `, ,,,Grand TOTAL ,,,,,,,,${totalBusStopT},${totalTravelKmT},${totalClaimT},${TravelGpsT},${totalClaimGpsT},,,,,,,,,,,,,0,${tAmount},${totallT + totallCT + totallST + totallOT},0,0,${totallT},${totallCT},${totallST},0,${totallOT},0,0\r\n\n`
        //

        const jsonResponse = JSON.stringify({ returnData: returnData2, exportData: data }).toString('base64');

        // Compress the JSON string
        const compressedData = zlib.gzipSync(jsonResponse);

        try {
            await saveToS3('justnaik-lambda-reports', key, compressedData);
            const jobQueue = await LambdaJobQueue.update('COMPLETED', jobId);
        } catch (error) { 
            const jobQueue = await LambdaJobQueue.update('FAILED', jobId);
            console.error('Error uploading to bucket: ', error);
            return {
                statusCode: 500,
                body: JSON.stringify({ message: 'Internal Server Error', error: error.message })
            };
        }

        return {
            statusCode: 200,
            body: compressedData
        };
    } catch (error) {
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Internal Server Error",
                error: error.message,
            }),
        };
    }
};
