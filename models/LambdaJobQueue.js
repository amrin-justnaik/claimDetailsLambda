import { Sql } from '../config/index.js';
import { Utils } from '../helpers/index.js';

class LambdaJobQueue {
    constructor(row) {
        this.id = row.id;
        this.jobId = row.job_id;
        this.key = row.key;
        this.status = row.status;
        this.createdAt = row.created_at;
        this.updatedAt = row.updated_at;
    }

    static async findOne(where = {}, runner = Sql) {
        const { whereQuery, sqlParams } = Utils.buildWhereQuery(where);
        const sqlQuery = `
            SELECT * FROM lambda_job_queue ${whereQuery}
        `;

        const response = await runner.query(sqlQuery, sqlParams);

        if (response.rows.length === 0) {
            return null;
        }

        return new LambdaJobQueue(response.rows[0]);
    }

    static async update(status, jobId, runner = Sql) {
        const sqlQuery = `
            UPDATE lambda_job_queue 
            SET status = $1, updated_at = NOW() 
            WHERE job_id = $2
            RETURNING *;
        `;

        const response = await runner.query(sqlQuery, [status, jobId]);

        if (response.rows.length === 0) {
            return null;
        }

        return new LambdaJobQueue(response.rows[0]);
    }
}

export default LambdaJobQueue;