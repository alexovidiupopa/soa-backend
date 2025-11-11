-- migrations/init.sql

CREATE DATABASE IF NOT EXISTS microdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE microdb;

-- users table (auth)
CREATE TABLE IF NOT EXISTS users (
                                     id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                     username VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- restaurants table
CREATE TABLE IF NOT EXISTS restaurants (
                                           id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    city VARCHAR(255),
    seats INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- bookings table
CREATE TABLE IF NOT EXISTS bookings (
                                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
    restaurant_id BIGINT NOT NULL,
    user VARCHAR(100) NOT NULL,
    people INT,
    when_ts TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE
    );

-- booking service local cache for restaurants (managed by booking consumer)
CREATE TABLE IF NOT EXISTS restaurants_cache (
                                                 id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    city VARCHAR(255),
    seats INT,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
