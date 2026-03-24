-- We delete the table, if it exists, to start fresh with each test
DROP TABLE IF EXISTS users;

-- We create the structure that JdbcTemplate expects
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);