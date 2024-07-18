CREATE TABLE propuesta (
    id SERIAL PRIMARY KEY,
    id_topology INTEGER,
    route_type VARCHAR(50),
    geom GEOMETRY(LINESTRING, 4326)
);
