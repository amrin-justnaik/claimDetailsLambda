import pkg from 'pg';
const { Pool } = pkg;
import dotenv from 'dotenv';
dotenv.config();

const isProduction = process.env.NODE_ENV === 'production'

// console.log('\n\n\n',process.env.DATABASE_URL,process.env.NODE_ENV,'\n\n\n')

const Sql = new Pool({
  connectionString: 'postgresql://justnaik:MDGvxKrhRWhj2C4HJUWE@justnaikdb.ckmfijp3sg7o.ap-southeast-1.rds.amazonaws.com:5432/justnaik',
  ssl: false,
})

export default Sql;