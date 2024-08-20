import pkg from 'pg';
const { Pool } = pkg;

// Replace with your database connection details
const connectionString = 'postgresql://justnaik:MDGvxKrhRWhj2C4HJUWE@justnaikdb.ckmfijp3sg7o.ap-southeast-1.rds.amazonaws.com:5432/justnaik';

// Create a new pool instance
const pool = new Pool({
    connectionString: connectionString,
    ssl: {
        rejectUnauthorized: false // Only for testing purposes; not recommended for production
    }
});

// Attempt to connect and query the database
pool.query('SELECT NOW()', (err, res) => {
    if (err) {
        console.error('Error connecting to the database:', err);
    } else {
        console.log('Connected to the database');
        console.log('Database response:', res.rows[0]);
    }
    
    // Close the pool
    pool.end();
});
