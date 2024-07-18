import os
import psycopg2
import networkx as nx
from geopy.distance import geodesic
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# Cargar la topología de la base de datos
cursor.execute("SELECT id_link, source, target FROM links WHERE id_topology=1")
links = cursor.fetchall()

# Cargar las coordenadas de los nodos
cursor.execute("SELECT id_node, lat, lon FROM nodes WHERE id_topology=1")
nodes = cursor.fetchall()

# Crear un diccionario de nodos con sus coordenadas
node_coords = {node[0]: (node[1], node[2]) for node in nodes}

# Crear el grafo con distancias geográficas como pesos
G = nx.Graph()
for link in links:
    source, target = link[1], link[2]
    if source in node_coords and target in node_coords:
        source_coords = node_coords[source]
        target_coords = node_coords[target]
        distance = geodesic(source_coords, target_coords).kilometers
        G.add_edge(source, target, weight=distance)

# Definir los nodos de origen y destino
source_node = 1  # Nodo de origen
target_node = 20  # Nodo de destino

# Función para obtener la geometría de la ruta
def get_route_geometry(route):
    # Crear un array de los nodos en la ruta
    node_ids = "{" + ",".join(map(str, route)) + "}"
    
    cursor.execute("""
        SELECT ST_AsText(ST_MakeLine(points.point)) 
        FROM (
            SELECT point
            FROM nodes, unnest(%s::int[]) WITH ORDINALITY as n(id, ord)
            WHERE nodes.id_node = n.id
            ORDER BY n.ord
        ) AS points
    """, (node_ids,))
    return cursor.fetchone()[0]

# Función para insertar la propuesta de ruta en la base de datos
def insert_route_proposal(route_type, route):
    route_geom = get_route_geometry(route)
    cursor.execute("""
        INSERT INTO propuesta (id_topology, route_type, geom) 
        VALUES (%s, %s, ST_GeomFromText(%s, 4326))
    """, (1, route_type, route_geom))
    conn.commit()

try:
    # Ruta primaria usando Dijkstra
    primary_route = nx.shortest_path(G, source=source_node, target=target_node, weight='weight')
    insert_route_proposal('primary', primary_route)
    
    # Ruta de respaldo eliminando los enlaces de la ruta primaria
    G_temp = G.copy()
    primary_edges = list(zip(primary_route[:-1], primary_route[1:]))
    G_temp.remove_edges_from(primary_edges)
    backup_route = nx.shortest_path(G_temp, source=source_node, target=target_node, weight='weight')
    insert_route_proposal('backup', backup_route)
except nx.NetworkXNoPath:
    print("No hay ruta disponible entre los nodos especificados")

# Cerrar la conexión a la base de datos
cursor.close()
conn.close()
