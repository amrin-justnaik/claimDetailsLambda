import { Sql, CONSTANTS } from '../config/index.js';
import { Utils } from '../helpers/index.js';

class RouteHistory {
    constructor(row) {
        this.id = row.id;
        // this.agencyId = row.agency_id;
        // this.agencyTripId = row.agency_trip_id;
        // this.userId = row.user_id;
        this.vehicleId = row.vehicle_id;
        this.vehicleRegistrationNumber = row.registration_number;
        this.routeId = row.route_id;
        this.driverId = row.driver_id;
        this.driverName = `${row.first_name} ${row.last_name}`
        this.routeShortName = row.short_name;
        this.journeyId = row.journey_id;
        this.tripId = row.trip_id;
        this.startStopName = row.start_stop_name;
        this.endStopName = row.end_stop_name;
        this.paymentType = row.payment_type;
        this.amount = Number(row.amount);
        this.isActive = row.is_active;
        this.createdAt = row.created_at;
        this.endedAt = row.ended_at;
        this.userCheckIn = row.user_checkin;
        this.userCheckOut = row.user_checkout;
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
    }
}





class Route {
    constructor(row) {
        this.id = row.id;
        this.name = row.name;
        this.shortName = row.short_name;
        this.agencyId = row.agency_id;
        this.polygon = row.polygon;
        this.colorCode = row.color_code;
        this.minBalance = Number(row.min_balance);
        this.isActive = row.is_active;
        this.createdAt = row.created_at;
        this.updatedAt = row.updated_at;
        this.isFixedFare = row.is_fixed_fare;
        this.isFreeFare = row.is_free_fare;
        this.childDiscount = row.child_discount
        this.foreignChildDiscount = row.foreign_child_discount
        this.seniorDiscount = row.senior_discount
        this.foreignAdultDiscount = row.foreign_adult_discount
        this.okuDiscount = row.oku_discount
        this.kmInbound = row.km_inbound,
        this.kmOutbound = row.km_outbound,
        this.kmLoop = row.km_loop,
        this.kmRate = row.km_charge,
        this.apadPolygon = row.apad_polygon,
        this.waypoints = row.route_waypoints,
        this.nameOutbound = row.name_outbound,
        this.nameInbound =  row.name_inbound
    }

    static async findAll(options, runner = Sql) {
        // const { sort = 'created_at DESC', limit = 30, skip = 0, ...where } = options;
        const { sort = 'created_at DESC', skip = 0, ...where } = options;
        const newWhere = {}
        Object.entries(where).forEach(([w, k]) => {
            newWhere[`r.${w}`] = k
        })
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(newWhere);
        const sqlQuery = `
            SELECT 
            r.id,
            r.name,
            r.short_name,
            r.agency_id,
            r.polygon,
            r.color_code,
            r.min_balance,
            r.is_active,
            r.created_at,
            r.updated_at,
            r.is_fixed_fare,
            r.is_free_fare,
            r.km_inbound,
            r.km_outbound,
            r.km_loop,
            r.km_charge,
            r.apad_polygon,
            r.name_outbound,
            r.name_inbound,
            COALESCE(w.route_waypoints, '[]') AS route_waypoints
            FROM 
                routes AS r
                LEFT JOIN LATERAL (
                    SELECT json_agg(json_build_object('stopId', w.stop_id, 'sequence', w.sequence)) AS route_waypoints
                    FROM waypoints AS w
                    WHERE  r.id = w.route_id
                ) w ON true
            ${whereQuery} 
            ORDER BY ${sort} 
            OFFSET ${skip}
            `;
        // LIMIT ${limit} 
        // console.log("sqlQuery",sqlQuery);
        // console.log("sqlParams",sqlParams);
        const response = await runner.query(sqlQuery, sqlParams);
        return response.rows.map(row => new Route(row));
    }

    static async findAllNoLimit(options, runner = Sql) {
        const { sort = 'created_at DESC', ...where } = options;
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);
        const sqlQuery = `
            SELECT * FROM routes ${whereQuery} ORDER BY ${sort}
        `;

        const response = await runner.query(sqlQuery, sqlParams);
        return response.rows.map(row => new Route(row));
    }

    static async findOne(where = {}, runner = Sql) {
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);
        const sqlQuery = `
            SELECT * FROM routes ${whereQuery}
        `;

        const response = await runner.query(sqlQuery, sqlParams);
        if (response.rows.length === 0) {
            return null;
        }
        return new Route(response.rows[0]);
    }

    static async create(name, shortName, agencyId, polygon, colorCode, minBalance = '0.00', isFixedFare = false, isFreeFare = false, kmInbound = null, kmOutbound = null, kmLoop = null, kmRate = null, apadPolygon = null,ibName= null,obName= null, runner = Sql) {
        if (minBalance === null) {
            minBalance = '0.00';
          }
        const sqlQuery = `
            INSERT INTO routes (name, short_name, agency_id, polygon, color_code, min_balance, is_fixed_fare, is_free_fare,km_inbound,km_outbound,km_loop,km_charge,apad_polygon,name_inbound,name_outbound) 
            values ($1, $2, $3, $4, $5, $6, $7, $8,$9,$10,$11,$12,$13,$14,$15)
            RETURNING *
        `;
        console.log("sqlQuery",sqlQuery);
        console.log("name, shortName, agencyId, polygon, colorCode, minBalance, isFixedFare, isFreeFare, kmInbound, kmOutbound, kmLoop, kmRate, apadPolygon,ibName,obName",name, shortName, agencyId, polygon, colorCode, minBalance, isFixedFare, isFreeFare, kmInbound, kmOutbound, kmLoop, kmRate, apadPolygon,ibName,obName);


        const response = await runner.query(sqlQuery, [name, shortName, agencyId, polygon, colorCode, minBalance, isFixedFare, isFreeFare, kmInbound, kmOutbound, kmLoop, kmRate, apadPolygon,ibName,obName]);

        if (response.rows.length === 0) {
            return null;
        }

        return new Route(response.rows[0]);
    }

    static async update(id, params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE routes SET ';
        let index = 1;
        let updateParams = [];

        for (let key in params) {
            if (typeof params[key] !== 'undefined') {
                sqlQuery += `${key} = $${index}, `;
                updateParams.push(params[key]);
                index++;
            }
        }

        updateParams.push(id);
        sqlQuery += `updated_at = NOW() WHERE id = $${index} RETURNING *`;
        const response = await runner.query(sqlQuery, updateParams);

        if (response.rows.length === 0) {
            return null;
        }

        return new Route(response.rows[0]);
    }

    static async getAgencyRouteHistory(agencyId, timestamp, options, runner = Sql) {
        // only get data between past 3 months until yesterday
        // contact super admin if want to get all of their transactions

        //DONT!! dont apply device id by vehicle id, device id currently can connect to more than 1 vehicle
        const { from, to } = options
        let sqlQuery = `
        SELECT
        r.id as route_id,
              r.id,
              r.short_name,
              r.is_active,
              t.route_id,
              t.vehicle_id,
              v.registration_number,
              t.driver_id,
              j.id as journey_id,
              t.id as trip_id,
              j.start_stop_id,
              j.end_stop_id,
              d.first_name,
              d.last_name,
              j.payment_type,
              j.amount,
              f.amount as adult_fare,
              f.child_amount as child_fare,
              f.senior_amount as senior_fare,
              f.oku_amount as oku_fare,
              f.foreign_adult_amount as f_adult_fare,
              f.foreign_child_amount as f_child_fare,
              t.created_at,
              t.ended_at,
              j.created_at as user_checkin,
              j.ended_at as user_checkout,
              j.no_of_pax,
              j.no_of_child,
              j.no_of_senior,
              j.no_of_oku,
              j.no_of_foreign_adult,
              j.no_of_foreign_child,
              dv.serial_number
          FROM
              routes AS r
               LEFT JOIN trips AS t ON t.route_id = r.id  
               LEFT JOIN journeys AS j ON j.trip_id = t.id
               LEFT JOIN drivers AS d ON t.driver_id = d.id
               LEFT JOIN vehicles AS v ON t.vehicle_id = v.id
               LEFT JOIN devices AS dv ON v.id = dv.vehicle_id
               LEFT JOIN fares AS f ON j.start_stop_id = f.origin_id and j.end_stop_id = f.destination_id
          WHERE
              r.agency_id = $1  
        `
        const queries = [agencyId]
        if (from && to) {
            sqlQuery += "AND t.created_at BETWEEN to_timestamp($2) AND to_timestamp($3);"
            queries.push(...[from, to])
        } else {
            sqlQuery += "AND t.created_at BETWEEN to_timestamp($2) - INTERVAL '3 months' AND to_timestamp($2);"
            queries.push(new Date(timestamp) / 1000)
        }
        const response = await runner.query(sqlQuery, queries);
        return response.rows.map(row => new RouteHistory(row));
    }
}


export default Route;