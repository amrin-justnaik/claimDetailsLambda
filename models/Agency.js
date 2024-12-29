import { Sql } from '../config/index.js';
import { Utils } from '../helpers/index.js';

class Agency {
    constructor(row) {
        this.id = row.id;
        this.name = row.name;
        this.imageUrl = row.image_url;
        this.isActive = row.is_active;
        this.createdAt = row.created_at;
        this.updatedAt = row.updated_at;
        this.maxVehicleCount = row.max_vehicle_count;
        this.maxDriverCount = row.max_driver_count;
        this.maxRouteCount = row.max_route_count;
        this.maxTripCount = row.max_trip_count;
        this.phoneNumber = row.phone_number;
        this.isEmailSubscribed = row.isEmailSubscribed;
        this.fleetSize = row.fleetSize;
        this.supervisorName = row.supervisor_name;
        this.supervisorPhoneNumber = row.supervisor_phone_number;
        this.supportHotline = row.support_hotline;
        this.supportEmail = row.support_email;
        this.isApadItg = row.is_apad_itg;
        this.isApadAccess = row.is_apad_access;
        this.isApadReport = row.is_apad_report_allow;
        this.apadUsername = row.apad_username;
        this.apadPassword = row.apad_password;
        this.fasspayDeveloperId = row.fp_developer_id;
        this.usingOfflineTrip = row.using_offline_trip;
    }

    static async findAll(where, runner = Sql) {
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);
        const sqlQuery = `SELECT * FROM agency ${whereQuery}`;

        const response = await runner.query(sqlQuery, sqlParams);

        if (response.rows.length === 0) {
            return null;
        }
        return response.rows.map(row => new Agency(row));
    }

    static async findOne(where, runner = Sql) {
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);
        const sqlQuery = `SELECT * FROM agency ${whereQuery}`;

        const response = await runner.query(sqlQuery, sqlParams);

        if (response.rows.length === 0) {
            return null;
        }
        return new Agency(response.rows[0]);
    }

    static async create(companyName, phone_number, isEmailSubscribed, fleetSize, runner = Sql) {
        let sqlQuery = `
            INSERT INTO agency (name, phone_number, "isEmailSubscribed", "fleetSize") 
            values ($1, $2, $3, $4) RETURNING *
        `;

        const response = await runner.query(sqlQuery, [
            companyName, phone_number, isEmailSubscribed, fleetSize
        ]);

        if (response.rows.length === 0) {
            return null;
        }

        return new Agency(response.rows[0]);
    }

    static async update(id, params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE agency SET ';
        let index = 1;
        let updateParams = [];

        // 
        for (let key in params) {
            if (typeof params[key] !== 'undefined') {
                if (key == 'fleetSize') {
                    sqlQuery += `"fleetSize" = $${index}, `;
                } else {
                    sqlQuery += `${key} = $${index}, `;
                }
                updateParams.push(params[key]);
                index++;
            }
        }

        updateParams.push(id);
        sqlQuery += `updated_at = NOW() WHERE id = $${index} RETURNING *`;

        // 
        const response = await runner.query(sqlQuery, updateParams);
        // 

        if (response.rows.length === 0) {
            return null;
        }

        return new Agency(response.rows[0]);
    }
}

export default Agency;