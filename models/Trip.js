import { Sql } from '../config/index.js';



class TripClaimHistory {
    constructor(row) {
        this.scheduledAt = row.scheduled_at;
        this.scheduledEndTime = row.scheduled_end_time;
        this.startedAt = row.started_at;
        this.endedAt = row.ended_at;
        this.id = row.id;
        this.agencyId = row.agency_id;
        this.agencyTripId = row.agency_trip_id;
        this.userId = row.user_id;
        this.vehicleId = row.vehicle_id;
        this.vehicleRegistrationNumber = row.registration_number;
        this.routeId = row.route_id;
        this.driverId = row.driver_id;
        this.driverIdentificationNumber = row.phone_number;
        this.obIb = row.direction_id;
        this.driverName = `${row.first_name} ${row.last_name}`
        this.routeShortName = row.short_name;
        this.routeName = row.route_name;
        this.journeyId = row.journey_id;
        this.tripId = row.trip_id;
        this.startStopName = row.start_stop_name;
        this.endStopName = row.end_stop_name;
        this.paymentType = row.payment_type;
        this.amount = Number(row.amount);
        this.isActive = row.is_active;
        this.createdAt = row.created_at;
        this.endedAt = row.ended_at;
        this.journeyCreated = row.j_created;
        this.journeyEnded = row.j_ended;
        this.noOfAdult = row.no_of_pax || 0; //adult
        this.noOfChild = row.no_of_child || 0;
        this.noOfSenior = row.no_of_senior || 0;
        this.noOfOku = row.no_of_oku || 0;
        this.noOfForeignAdult = row.no_of_foreign_adult || 0;
        this.noOfForeignChild = row.no_of_foreign_child || 0;
        this.adultFare = row.adult_fare;
        this.childFare = row.child_fare;
        this.seniorFare = row.senior_fare;
        this.okuFare = row.oku_fare;
        this.foreignAdultFare = row.f_adult_fare;
        this.foreignChildFare = row.f_child_fare;
        this.deviceSerialNumber = row.serial_number;
        this.apadPolygon = row.apad_polygon,
            this.kmOutbound = row.km_outbound,
            this.kmInbound = row.km_inbound,
            this.kmLoop = row.km_loop,
            this.kmRate = row.km_charge
        this.firstStop = row.first_stop
        this.VehicleAge = row.vehicle_age
        this.staffId = row.staff_id
        this.trip_mileage = row.trip_mileage

    }
}


class Trip {
    constructor(row) {
        this.id = row.id;
        this.agencyTripId = row.agency_trip_id;
        this.routeId = row.route_id;
        this.routeColor = row.color_code;
        this.routeName = row.name;
        this.routeShortName = row.short_name;
        this.driverId = row.driver_id;
        this.directionId = row.direction_id;
        this.driverName = row.driver_first_name ? `${row.driver_first_name} ${row.driver_last_name}` : null;
        this.driverNameAssigned = row.driver_first_name_assigned ? `${row.driver_first_name_assigned} ${row.driver_last_name_assigned}` : null;
        this.vehicleId = row.vehicle_id;
        this.vehicleRegistrationNumber = row.vehicle_registration_number;
        this.vehicleRegistrationNumberAssigned = row.vehicle_registration_number_assigned;
        this.endedAt = row.ended_at;
        this.createdAt = row.created_at;
        this.updatedAt = row.updated_at;
        this.scheduledAt = row.scheduled_at;
        this.startedAt = row.started_at;
        this.startOdometer = row.start_odometer
        this.endOdometer = row.end_odometer
        this.startSoc = row.start_soc
        this.endSoc = row.end_soc
        this.totalNoOfPax = row.total_no_of_pax
        this.totalNoOfChild = row.total_no_of_child
        this.totalNoOfSenior = row.total_no_of_senior
        this.totalNoOfOku = row.total_no_of_oku
        this.totalNoOfForeignAdult = row.total_no_of_foreign_adult
        this.totalNoOfForeignChild = row.total_no_of_foreign_child
        this.isActive = row.is_active
        this.obIb = row.direction_id;
        this.agencyId = row.agency_id;
        this.km_outbound = row.km_outbound
        this.km_inbound = row.km_inbound
        this.km_loop = row.km_loop
        this.trip_mileage = row.trip_mileage
    }

    static async getAgencyClaimDetailsReportByDate(agencyId, routeIds, options, runner = Sql) {
        // only get data between past 1 months until yesterday
        // contact super admin if want to get all of their transactions

        //DONT!! dont apply device id by vehicle id, device id currently can connect to more than 1 vehicle
        const { from, to } = options
        let sqlQuery = `
            SELECT *
            FROM (
                SELECT DISTINCT ON (j.id)
                    r.id,
                    r.short_name,
                    r.name AS route_name,
                    t.direction_id,
                    t.id AS trip_id,
                    t.started_at,
                    t.ended_at,
                    t.scheduled_at,
                    d.phone_number,
                    t.agency_trip_id,
                    t.scheduled_end_time,
                    j.user_id,
                    t.route_id,
                    t.vehicle_id,
                    v.registration_number,
                    t.driver_id,
                    j.id AS journey_id,
                    j.start_stop_id,
                    j.end_stop_id,
                    d.first_name,
                    d.last_name,
                    d.staff_id,
                    j.payment_type,
                    j.amount,
                    f.amount AS adult_fare,
                    f.child_amount AS child_fare,
                    f.senior_amount AS senior_fare,
                    f.oku_amount AS oku_fare,
                    f.foreign_adult_amount AS f_adult_fare,
                    f.foreign_child_amount AS f_child_fare,
                    t.created_at,
                    t.ended_at,
                    t.trip_mileage,
                    j.created_at AS j_created,
                    j.ended_at AS j_ended,
                    j.no_of_pax,
                    j.no_of_child,
                    j.no_of_senior,
                    j.no_of_oku,
                    j.no_of_foreign_adult,
                    j.no_of_foreign_child,
                    dv.serial_number,
                    r.apad_polygon,
                    r.km_outbound,
                    r.km_inbound,
                    r.km_loop,
                    r.km_charge,
                    s.name AS first_stop,
                    v.vehicle_age
                FROM
                    trips AS t
                    LEFT JOIN journeys AS j ON j.trip_id = t.id
                    LEFT JOIN routes AS r ON t.route_id = r.id
                    LEFT JOIN waypoints AS w ON w.route_id = r.id
                    LEFT JOIN stops AS s ON s.id = w.stop_id AND w.route_id = r.id
                    LEFT JOIN drivers AS d ON t.driver_id = d.id
                    LEFT JOIN vehicles AS v ON t.vehicle_id = v.id
                    LEFT JOIN devices AS dv ON v.id = dv.vehicle_id
                    LEFT JOIN fares AS f ON j.start_stop_id = f.origin_id AND j.end_stop_id = f.destination_id
                    LEFT JOIN agency AS ag ON ag.id = r.agency_id
                WHERE
                    r.agency_id = $1
                    AND w.sequence = 1
                    AND ag.is_apad_report_allow = TRUE
                    AND t.is_apad_active = TRUE
        `;

        const queries = [agencyId];
        let paramIndex = 2;

        if (from && to) {
            sqlQuery += "AND (((t.scheduled_at BETWEEN (to_timestamp($2) AT TIME ZONE 'UTC+8') AND (to_timestamp($3) AT TIME ZONE 'UTC+8')) AND (t.scheduled_end_time BETWEEN (to_timestamp($2) AT TIME ZONE 'UTC+8') AND (to_timestamp($3) AT TIME ZONE 'UTC+8'))) OR (t.started_at BETWEEN (to_timestamp($2) AT TIME ZONE 'UTC+8') AND (to_timestamp($3) AT TIME ZONE 'UTC+8')))"
            queries.push(...[new Date(from) / 1000, new Date(to) / 1000]);

            paramIndex += 2;
        } else {
            sqlQuery += "AND (((DATE(TIMEZONE('UTC+8', t.scheduled_at)) >= date_trunc('month', now()) - interval '2 month') AND (DATE(TIMEZONE('UTC+8', t.scheduled_end_time))  >= date_trunc('month', now()) - interval '2 month')) OR (DATE(TIMEZONE('UTC+8', t.started_at))  >= date_trunc('month', now()) - interval '2 month'))"
            // queries.push(new Date(timestamp) / 1000)
            //DATE(TIMEZONE('UTC+8', t.scheduled_at))
        }

        if (Array.isArray(routeIds) && routeIds.length > 0) {
            sqlQuery += `AND r.id = ANY($${paramIndex})`;
            queries.push(routeIds);
        }

        sqlQuery += `
        ) AS distinct_journeys
            UNION ALL
            SELECT *
            FROM (
                SELECT
                    r.id,
                    r.short_name,
                    r.name AS route_name,
                    t.direction_id,
                    t.id AS trip_id,
                    t.started_at,
                    t.ended_at,
                    t.scheduled_at,
                    d.phone_number,
                    t.agency_trip_id,
                    t.scheduled_end_time,
                    j.user_id,
                    t.route_id,
                    t.vehicle_id,
                    v.registration_number,
                    t.driver_id,
                    j.id AS journey_id,
                    j.start_stop_id,
                    j.end_stop_id,
                    d.first_name,
                    d.last_name,
                    d.staff_id,
                    j.payment_type,
                    j.amount,
                    f.amount AS adult_fare,
                    f.child_amount AS child_fare,
                    f.senior_amount AS senior_fare,
                    f.oku_amount AS oku_fare,
                    f.foreign_adult_amount AS f_adult_fare,
                    f.foreign_child_amount AS f_child_fare,
                    t.created_at,
                    t.ended_at,
                    t.trip_mileage,
                    j.created_at AS j_created,
                    j.ended_at AS j_ended,
                    j.no_of_pax,
                    j.no_of_child,
                    j.no_of_senior,
                    j.no_of_oku,
                    j.no_of_foreign_adult,
                    j.no_of_foreign_child,
                    dv.serial_number,
                    r.apad_polygon,
                    r.km_outbound,
                    r.km_inbound,
                    r.km_loop,
                    r.km_charge,
                    s.name AS first_stop,
                    v.vehicle_age
                FROM
                    trips AS t
                    LEFT JOIN journeys AS j ON j.trip_id = t.id
                    LEFT JOIN routes AS r ON t.route_id = r.id
                    LEFT JOIN waypoints AS w ON w.route_id = r.id
                    LEFT JOIN stops AS s ON s.id = w.stop_id AND w.route_id = r.id
                    LEFT JOIN drivers AS d ON t.driver_id = d.id
                    LEFT JOIN vehicles AS v ON t.vehicle_id = v.id
                    LEFT JOIN devices AS dv ON v.id = dv.vehicle_id
                    LEFT JOIN fares AS f ON j.start_stop_id = f.origin_id AND j.end_stop_id = f.destination_id
                    LEFT JOIN agency AS ag ON ag.id = r.agency_id
                WHERE
                    r.agency_id = $1
                    AND w.sequence = 1
                    AND ag.is_apad_report_allow = TRUE
                    AND t.is_apad_active = TRUE
                    AND j.id IS NULL
        `;

        if (from && to) {
            sqlQuery += "AND (((t.scheduled_at BETWEEN (to_timestamp($2) AT TIME ZONE 'UTC+8') AND (to_timestamp($3) AT TIME ZONE 'UTC+8')) AND (t.scheduled_end_time BETWEEN (to_timestamp($2) AT TIME ZONE 'UTC+8') AND (to_timestamp($3) AT TIME ZONE 'UTC+8'))) OR (t.started_at BETWEEN (to_timestamp($2) AT TIME ZONE 'UTC+8') AND (to_timestamp($3) AT TIME ZONE 'UTC+8')))"
        } else {
            sqlQuery += "AND (((DATE(TIMEZONE('UTC+8', t.scheduled_at)) >= date_trunc('month', now()) - interval '2 month') AND (DATE(TIMEZONE('UTC+8', t.scheduled_end_time))  >= date_trunc('month', now()) - interval '2 month')) OR (DATE(TIMEZONE('UTC+8', t.started_at))  >= date_trunc('month', now()) - interval '2 month'))"
        }

        if (Array.isArray(routeIds) && routeIds.length > 0) {
            sqlQuery += `AND r.id = ANY($${paramIndex})`;
        }

        sqlQuery += `) AS all_null_journeys;`;

        const response = await runner.query(sqlQuery, queries);
        return response.rows.map(row => new TripClaimHistory(row));
    }
}

export default Trip;