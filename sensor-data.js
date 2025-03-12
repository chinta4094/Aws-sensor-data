require('dotenv').config();
const mqtt = require('mqtt');
const fs = require('fs');
const { Pool } = require('pg');

// PostgreSQL Database Connection
const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    ssl: {
        rejectUnauthorized: false // Bypass self-signed certificates (use true in production)
    }
});

pool.connect((err) => {
    if (err) {
        console.error("Database connection error:", err);
    } else {
        console.log("Connected to PostgreSQL");
    }
});

const options = {
    key: fs.readFileSync(process.env.PRIVATE_KEY_PATH),
    cert: fs.readFileSync(process.env.CERTIFICATE_PATH),
    ca: fs.readFileSync(process.env.ROOT_CA_PATH),
    protocol: 'mqtts'
};

const client = mqtt.connect(`mqtts://${process.env.AWS_IOT_ENDPOINT}`, options);
const topic = process.env.TOPIC || "iot/sensor/data";  // Default topic

client.on('connect', () => {
    console.log(`Connected to AWS IoT. Publishing to topic: ${topic}`);
    let interval = 1;

    setInterval(async () => {
        const totalSpots = 100; // Fixed total cunt

        // Generate random values while ensuring their sum is 100
        const occupied = Math.floor(Math.random() * 40) + 10; // 10 to 50
        const reserved = Math.floor(Math.random() * (50 - occupied)); // Adjust dynamically
        const faultySensors = Math.floor(Math.random() * (50 - occupied - reserved)); // Adjust dynamically
        const available = totalSpots - (occupied + reserved + faultySensors); // Ensure sum = 100

        const payload = JSON.stringify({
            id: 'B1PS-001',
            deviceName: "B1-parkingSensor",
            occupied,
            reserved,
            available,
            faultySensors,
            total: totalSpots,
            timestamp: new Date().toISOString()
        });

        // Publish to AWS IoT
        client.publish(topic, payload, { qos: 0 }, async (err) => {
            if (err) {
                console.error("Publish error:", err);
            } else {
                console.log(`Data sent: ${interval++}`, payload);

                // Insert data into PostgreSQL
                try {
                    await pool.query(
                        `INSERT INTO sensor_data (device_id, device_name, occupied, reserved, available, faulty_sensors, total, timestamp) 
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                        ['B1PS-001', 'B1-parkingSensor', occupied, reserved, available, faultySensors, totalSpots, new Date()]
                    );
                    console.log("Data inserted into PostgreSQL");
                } catch (dbError) {
                    console.error("Database insert error:", dbError);
                }
            }
        });
    }, 1 * 60 * 1000); // Sends data every 1 min
});

client.on('error', (err) => {
    console.error("Connection error:", err);
});
