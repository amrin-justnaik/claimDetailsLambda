import { Sql } from '../config/index.js';
import { Utils } from '../helpers/index.js';
var format = require('pg-format');

class ScheduleV2 {
    constructor(row) {
        this.id = row.id;
        this.uuid = row.uuid;
        this.schedule_id = row.schedule_id;
        this.name = row.name;
        this.day = row.day;
        this.start_time = row.start_time;
        this.end_time = row.end_time;
        this.agency_id = row.agency_id;
        this.is_active = row.is_active;
        this.created_at = row.created_at;
        this.updated_at = row.updated_at
    }
    static async create(data, runner = Sql) {
        // 
        const sqlQuery = format('INSERT INTO schedulesv2 (day,uuid,start_time,end_time,is_active,schedule_id,agency_id, name) VALUES %L RETURNING *', data)
        // const sqlQuery = `
        //     INSERT INTO schedules (route_id, schedule, frequency) 
        //     values ($1, $2, $3)
        //     RETURNING *
        // `;
        // 

        const response = await runner.query(sqlQuery);

        if (response.rows.length === 0) {
            return null;
        }

        return new ScheduleV2(response.rows[0]);
    }
    static async create2(data, runner = Sql) {
        const sqlQuery = format('INSERT INTO schedulesv2 (day,uuid,start_time,end_time,is_active,schedule_id,agency_id, name) VALUES %L RETURNING *', data)
        // 
        
        const response = await runner.query(sqlQuery);

        if (response.rows.length === 0) {
            return null;
        }
        // 

        return response.rows;
    }
    static async findAllDistinct(options, runner = Sql) {
        const { ...where } = options;

        const newWhere = {}
        Object.entries(where).forEach(([w, k]) => {
            newWhere[`sc.${w}`] = k
        })
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(newWhere);
        const sqlQuery = `
            SELECT  DISTINCT on (sc.schedule_id)
            sc.id,
            sc.uuid,
            sc.schedule_id,
            sc.name,
            sc.day,
            sc.start_time,
            sc.end_time,
            sc.agency_id,
            sc.is_active,
            sc.created_at,
            sc.updated_at
            FROM 
            schedulesv2 AS sc
            ${whereQuery} 
            ORDER BY 
            sc.schedule_id, sc.created_at ASC;
            `;
        // LIMIT ${limit} 
        // 
        const response = await runner.query(sqlQuery, sqlParams);
        return response.rows.map(row => new ScheduleV2(row));
    }
    static async delete(options, runner = Sql) {
        const { ...where } = options;

        const newWhere = {}
        Object.entries(where).forEach(([w, k]) => {
            newWhere[`sc.${w}`] = k
        })
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(newWhere);
        const sqlQuery = `
            DELETE
            FROM 
            schedulesv2 AS sc
            ${whereQuery} 
    
            `;
        // LIMIT ${limit} 
        // 
        const response = await runner.query(sqlQuery, sqlParams);
        return response.rows.map(row => new ScheduleV2(row));
    }

    static async findAll(options, runner = Sql) {
        // const { sort = 'created_at DESC', limit = 30, skip = 0, ...where } = options;
        const { ...where } = options;
        const newWhere = {}
        Object.entries(where).forEach(([w, k]) => {
            newWhere[`sc.${w}`] = k
        })
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(newWhere);
        const sqlQuery = `
        SELECT
        sc.id,
        sc.uuid,
        sc.schedule_id,
        sc.name,
        sc.day,
        sc.start_time,
        sc.end_time,
        sc.agency_id,
        sc.is_active
        FROM 
        schedulesv2 AS sc
        ${whereQuery} 
            `;
        // LIMIT ${limit} 
        // 

        const response = await runner.query(sqlQuery, sqlParams);
        // 
        return response.rows.map(row => new ScheduleV2(row));
    }
    static async findAll2(options, runner = Sql) {
        // const { sort = 'created_at DESC', limit = 30, skip = 0, ...where } = options;
        const { ...where } = options;
        const newWhere = {}
        Object.entries(where).forEach(([w, k]) => {
            newWhere[`sc.${w}`] = k
        })
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(newWhere);
        const sqlQuery = `
        SELECT
        sc.id,
        sc.uuid,
        sc.schedule_id,
        sc.name,
        sc.day,
        sc.start_time,
        sc.end_time,
        sc.agency_id,
        sc.is_active
        FROM 
        schedulesv2 AS sc
        ${whereQuery} 
            `;
        // LIMIT ${limit} 
        // 

        const response = await runner.query(sqlQuery, sqlParams);
        // 
        return response.rows;
    }
    static async update(id, name, params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE schedulesv2 SET ';
        let index = 1;
        let updateParams = [];

        for (let key in params) {
            if (typeof params[key] !== 'undefined') {
                sqlQuery += `${key} = $${index} `;
                updateParams.push(params[key]);
                index++;
            }
        }

        updateParams.push(id);
        updateParams.push(name);
        sqlQuery += `,updated_at = NOW() WHERE schedule_id = $${index} AND day = $${index + 1} RETURNING *`;
        // 
        const response = await runner.query(sqlQuery, updateParams);
        if (response.rows.length === 0) {
            return null;
        }

        return new ScheduleV2(response.rows[0]);
    }
    static async updateTime(id,params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE schedulesv2 SET ';
        let index = 1;
        let updateParams = [];

        for (let key in params) {
            if (typeof params[key] !== 'undefined'  && key != 'start_time') {
                sqlQuery += `${key} = $${index} `;
                updateParams.push(params[key]);
                index++;
            }
            if (typeof params[key] !== 'undefined' && key != 'end_time') {
                sqlQuery += `${key} = $${index}, `;
                updateParams.push(params[key]);
                index++;
            }
        }

        updateParams.push(id);

        sqlQuery += `,updated_at = NOW() WHERE uuid = $${index} RETURNING *`;
        // 
        const response = await runner.query(sqlQuery, updateParams);
        if (response.rows.length === 0) {
            return null;
        }

        return new ScheduleV2(response.rows[0]);
    }
    static async updateName(id, params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE schedulesv2 SET ';
        let index = 1;
        let updateParams = [];

        for (let key in params) {
            if (typeof params[key] !== 'undefined') {
                sqlQuery += `${key} = $${index} `;
                updateParams.push(params[key]);
                index++;
            }
        }

        updateParams.push(id);
        sqlQuery += `,updated_at = NOW() WHERE schedule_id = $${index} RETURNING *`;
        // 
        const response = await runner.query(sqlQuery, updateParams);
        if (response.rows.length === 0) {
            return null;
        }

        return new ScheduleV2(response.rows[0]);
    }
    static async update2(id, schedule_id, params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE schedulesv2 SET ';
        let index = 1;
        let updateParams = [];

        // 
        for (let key in params) {
            if (typeof params[key] !== 'undefined') {
                if (key == 'fleetSize') {
                    sqlQuery += `"fleetSize" = $${index}, `;
                } else {
                    sqlQuery += `${key} = $${index} `;
                }
                updateParams.push(params[key]);
                index++;
            }
        }

        updateParams.push(id);
        updateParams.push(schedule_id);
        sqlQuery += `,updated_at = NOW() WHERE agency_id = $${index} AND id = $${index + 1}  RETURNING *`;

        // 
        // 
        const response = await runner.query(sqlQuery, updateParams);

        if (response.rows.length === 0) {
            return null;
        }

        return new ScheduleV2(response.rows[0]);
    }

    // static async findAll(options, runner = Sql) {      
    //     // const { sort = 'created_at DESC', limit = 30, skip = 0, ...where } = options;
    //     const { sort = 'created_at DESC', skip = 0, ...where } = options;
    //     const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);         
    //     const sqlQuery = `
    //         SELECT * FROM schedules ${whereQuery} ORDER BY ${sort} OFFSET ${skip}
    //         `;        
    //     // SELECT * FROM schedules ${whereQuery} ORDER BY ${sort} LIMIT ${limit} OFFSET ${skip}

    //     const response = await runner.query(sqlQuery, sqlParams);        
    //     return response.rows.map(row => new Schedule(row));
    // }

    // static async findOne(where = {}, runner = Sql) {        
    //     const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);   
    //     const sqlQuery = `
    //         SELECT * FROM schedules ${whereQuery}
    //     `; 

    //     const response = await runner.query(sqlQuery, sqlParams);    

    //     if (response.rows.length === 0) {
    //         return null;
    //     }        
    //     return new Schedule(response.rows[0]);
    // }


    // static async update(routeId, params, runner = Sql) {
    //     if (typeof params !== 'object') return null;

    //     let sqlQuery = 'UPDATE schedules SET ';
    //     let index = 1;   
    //     let updateParams = [];

    //     for (let key in params) {
    //         if (typeof params[key] !== 'undefined') {
    //             sqlQuery += `${key} = $${index}, `;
    //             updateParams.push(params[key]);
    //             index ++;
    //         }
    //     }

    //     updateParams.push(routeId);
    //     sqlQuery += `updated_at = NOW() WHERE route_id = $${index} RETURNING *`;

    //     const response = await runner.query(sqlQuery, updateParams);    

    //     if (response.rows.length === 0) {
    //         return null;
    //     }        

    //     return new Schedule(response.rows[0]);
    // }
}

export default ScheduleV2;