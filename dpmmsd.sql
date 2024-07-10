CREATE TABLE routes_dpmmsd (
    id SERIAL PRIMARY KEY,
    id_topology INTEGER,
    route_type VARCHAR(255),
    geom GEOMETRY(LINESTRING, 4326)
);
