import { Sql } from '../config/index.js';
import { Utils } from '../helpers/index.js';

class Stop {
    constructor(row){
        this.id = row.id;
        this.name = row.name;   
        this.address = row.address;               
        this.latitude = row.latitude;   
        this.longitude = row.longitude;   
        this.agencyId = row.agency_id;
        this.sequence = row.sequence;        
        this.directionId = row.direction_id;
        this.isCanSkip = row.is_can_skip;
        this.hasNearbyStop = row.has_nearby_stop;
        this.isActive = row.is_active;
        this.createdAt = row.created_at;
        this.updatedAt = row.updated_at;
        this.stopId = row.stop_id
        this.stopIdPublic = row.stop_id_public
        this.waypointId = row.waypoint_id;
        this.routeId = row.route_id
    }
    
    static async findOne(where, runner = Sql) {
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);   
        const sqlQuery = `SELECT 
            id, name, address, ST_X(coordinates) AS latitude, ST_Y(coordinates) AS longitude, 
            agency_id, is_active, created_at, updated_at 
            FROM stops ${whereQuery}
        `;
    
        const response = await runner.query(sqlQuery, sqlParams);    
        
        if (response.rows.length === 0) {
            return null;
        }        
        return new Stop(response.rows[0]);
    }

    static async findAll(where, runner = Sql) {
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);                   
        const sqlQuery = `SELECT 
            id, name, address, ST_X(coordinates) AS latitude, ST_Y(coordinates) AS longitude, 
            agency_id, is_active, created_at, updated_at,stop_id,stop_id_public 
            FROM stops ${whereQuery} ORDER BY id ASC;
        `;
    
        const response = await runner.query(sqlQuery, sqlParams);    
         
        return response.rows.map(row => new Stop(row));
    }

    static async findByRoutes (routeId, runner = Sql) {               
        const sqlQuery = `
            SELECT
                s.id,
                w.id AS waypoint_id,
                s.name,
                s.address,
                ST_X(s.coordinates) AS latitude,
                ST_Y(s.coordinates) AS longitude,
                s.agency_id,
                s.is_active,
                w.sequence,
                w.direction_id,
                w.is_can_skip,
                w.has_nearby_stop,
                s.created_at,
                s.updated_at,
                w.route_id
            FROM
                waypoints AS w
                LEFT JOIN stops AS s ON s.id = w.stop_id
            WHERE
                w.route_id = $1
                
            ORDER BY
                w.sequence ASC;
        `;                
        
        const response = await runner.query(sqlQuery, [routeId]);          
        return response.rows.map(row => new Stop(row));        
    }

    static async create(name, address, latitude, longitude,stopId = null,stopIdPublic = null, agencyId, runner = Sql) {
        const sqlQuery = `
            INSERT INTO stops (name, address, coordinates, agency_id,stop_id,stop_id_public) 
            values ($1, $2, ST_MakePoint($3, $4), $5,$6,$7) RETURNING *
        `;
        
        const response = await runner.query(sqlQuery, [name, address, latitude, longitude, agencyId,stopId,stopIdPublic]);            

        if (response.rows.length === 0) {
            return null;
        }
        
        return await Stop.findOne({ id: response.rows[0].id });
    }

    static async update(id, params, runner = Sql) {
        if (typeof params !== 'object') return null;

        let sqlQuery = 'UPDATE stops SET ';
        let index = 1;   
        let updateParams = [];

        for (let key in params) {
            if (typeof params[key] !== 'undefined' && key !== 'coordinates') {
                sqlQuery += `${key} = $${index}, `;
                updateParams.push(params[key]);
                index ++;
            } else if (key === 'coordinates' && params[key].latitude && params[key].longitude) {
                sqlQuery += `${key} = ST_MakePoint($${index}, $${index + 1}), `;
                updateParams.push(params[key].latitude);
                updateParams.push(params[key].longitude);
                index = index + 2;
            }            
        }
        
        updateParams.push(id);
        sqlQuery += `updated_at = NOW() WHERE id = $${index} RETURNING *`;
        
        const response = await runner.query(sqlQuery, updateParams);    
        
        if (response.rows.length === 0) {
            return null;
        }        

        return new Stop(response.rows[0]);
    }
}

export default Stop;