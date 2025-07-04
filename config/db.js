import pkg from 'pg';
const { Pool } = pkg;
import dotenv from 'dotenv';
dotenv.config();

const isProduction = process.env.NODE_ENV === 'production'

// console.log('\n\n\n',process.env.DATABASE_URL,process.env.NODE_ENV,'\n\n\n')

const Sql = new Pool({
    connectionString: process.env.DB_URL,
    ssl: {
        rejectUnauthorized: false
    },
})

export default Sql;