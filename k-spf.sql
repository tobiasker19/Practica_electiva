CREATE TABLE routes_kspf (
    id SERIAL PRIMARY KEY,
    id_topology INTEGER NOT NULL,
    source INTEGER NOT NULL,
    target INTEGER NOT NULL,
    path_type VARCHAR(50) NOT NULL, -- 'primary' o 'backup'
    geom GEOMETRY(LINESTRING, 4326) NOT NULL,
    FOREIGN KEY (id_topology) REFERENCES topology(id_topology),
    FOREIGN KEY (id_topology, source) REFERENCES nodes(id_topology, id_node),
    FOREIGN KEY (id_topology, target) REFERENCES nodes(id_topology, id_node)
);
