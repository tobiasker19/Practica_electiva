CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    id_topology INTEGER,
    route_type VARCHAR(50),
    geom GEOMETRY(LINESTRING, 4326),
    FOREIGN KEY (id_topology) REFERENCES topology(id_topology)
);
