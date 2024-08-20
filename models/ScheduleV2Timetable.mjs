import { Sql } from '../config/index.js';
import { Utils } from '../helpers/index.js';


class ScheduleV2Timetable {
    constructor(row) {
        // this.id = row.id;
        // this.agency_id = row.agency_id;
        // this.route_id = row.route_id;
        // this.direction_id = row.direction_id;
        // this.schedule_id = row.schedule_id;
        // if (row.driver_id) {

        //     this.driver_id = row.driver_id;
        // }
        // if (row.vehicle_id) {

        //     this.vehicle_id = row.vehicle_id;
        // }
        this.id = row.id,
            this.schedule_uuid = row.uuid,
            this.schedule_id = row.schedule_id,
            this.name = row.name,
            this.day = row.day,
            this.start_time = row.start_time,
            this.end_time = row.end_time,
            this.agency_id = row.agency_id,
            this.is_active = row.is_active,

            this.route_id = row.route_id,
            this.direction_id = row.direction_id,
            this.start_stop_id = row.start_stop_id,
            this.end_stop_id = row.end_stop_id,

            this.start_stop_name = row.start_stop_name,
            this.end_stop_name = row.end_stop_name,
            this.driver_id = row.driver_id,
            this.vehicle_id = row.vehicle_id,
            this.driver_frist_name = row.d_first_name,
            this.driver_last_name = row.d_last_name,
            this.vehicle_registration_number = row.registration_number

    }

    static async findAllDistinct(options, runner = Sql) {
        const { ...where } = options;

        const newWhere = {}
        Object.entries(where).forEach(([w, k]) => {
            newWhere[`sct.${w}`] = k
        })
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(newWhere);
        const sqlQuery = `
            SELECT  
            sct.id AS uuid,
            sct.schedulesv2_schedule_id AS schedule_id,
            sct.schedulesv2_name AS name,
            sct.schedulesv2_day AS day,
            sct.schedulesv2_start_time AS start_time,
            sct.schedulesv2_end_time AS end_time,
            sct.schedulesv2_agency_id AS agency_id,
            sct.schedulesv2_is_active AS is_active,
            sct.id,
            sct.route_id,
            sct.direction_id,
            sct.start_stop_id,
            sct.end_stop_id,
            s.name AS start_stop_name,
            s2.name AS end_stop_name,
            sct.driver_id,
            sct.vehicle_id,
            d.first_name AS d_first_name,
            d.last_name AS d_last_name,
            v.registration_number
            FROM 
            schedulesv2timetable AS sct
            LEFT JOIN stops AS s ON s.id = sct.start_stop_id
            LEFT JOIN stops AS s2 ON s2.id = sct.end_stop_id
            LEFT JOIN drivers AS d ON d.id = sct.driver_id
            LEFT JOIN vehicles AS v ON v.id = sct.vehicle_id
            ${whereQuery} 
    
            `;
        // LIMIT ${limit} 
        // 
        const response = await runner.query(sqlQuery, sqlParams);
        return response.rows.map(row => new ScheduleV2Timetable(row));
    }

}


export default ScheduleV2Timetable;