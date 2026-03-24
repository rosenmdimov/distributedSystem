-- Изтриваме таблицата, ако съществува, за да почваме на чисто при всеки тест
DROP TABLE IF EXISTS users;

-- Създаваме структурата, която JdbcTemplate очаква
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);